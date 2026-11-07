use crate::py_pipeline_wrapper::PyPipelineWrapper;
use floq::pipeline::{Message, Operator, PipelineTask, Receiver, Sender, Sink};
use floq::transformers::{AdkAgent, AdkInput, AdkSessionMode};
use floq::{impl_operator_component, impl_sink_component, Error, Result};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

static PY_EPHEMERAL_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Build the Python helper function once on the main thread during `AdkLiveSink.__new__`
/// so worker threads never invoke `PyModule::from_code` or Python's import machinery.
fn create_push_helper(py: Python<'_>) -> PyResult<PyObject> {
    let helper_mod = PyModule::from_code(
        py,
        r#"
try:
    from google.genai import types as _genai_types
except ImportError:
    _genai_types = None

def push_adk_content(queue, role, text):
    if _genai_types is not None:
        part = _genai_types.Part.from_text(text=text)
        content = _genai_types.Content(role=role, parts=[part])
    else:
        content = {"role": role, "parts": [{"text": text}]}
    queue.send_content(content)
"#,
        "_floq_adk_helper.py",
        "_floq_adk_helper",
    )?;
    Ok(helper_mod.getattr("push_adk_content")?.into())
}

/// Push formatted prompt text into `queue` as a `google.genai.types.Content` object.
fn dispatch_send_content(
    py: Python<'_>,
    event_loop: Option<&PyObject>,
    push_fn: &PyObject,
    queue: &PyObject,
    role: &str,
    text: &str,
) -> PyResult<()> {
    if let Some(loop_obj) = event_loop {
        let is_running = loop_obj
            .call_method0(py, "is_running")
            .and_then(|v| v.extract::<bool>(py))
            .unwrap_or(false);
        if is_running {
            loop_obj.call_method1(
                py,
                "call_soon_threadsafe",
                (push_fn.clone_ref(py), queue.clone_ref(py), role, text),
            )?;
            return Ok(());
        }
    }

    push_fn.call1(py, (queue.clone_ref(py), role, text))?;
    Ok(())
}

/// Dispatch `method_name()` via `loop.call_soon_threadsafe` if an `asyncio` event loop
/// is active, or directly on the current thread otherwise.
fn dispatch_threadsafe_noarg(
    py: Python<'_>,
    event_loop: Option<&PyObject>,
    target: &PyObject,
    method_name: &str,
) -> PyResult<()> {
    let method = target.getattr(py, method_name)?;
    if let Some(loop_obj) = event_loop {
        let is_running = loop_obj
            .call_method0(py, "is_running")
            .and_then(|v| v.extract::<bool>(py))
            .unwrap_or(false);
        if is_running {
            loop_obj.call_method1(py, "call_soon_threadsafe", (method,))?;
            return Ok(());
        }
    }

    method.call0(py)?;
    Ok(())
}

/// Shared configuration for [`AdkLiveSinkComponent`].
pub struct AdkLiveSinkConfig {
    pub live_request_queue: PyObject,
    pub push_fn: PyObject,
    pub prompt_prefix: Option<String>,
    pub separator: String,
    pub role: String,
    pub session_key_fn: Option<PyObject>,
    pub close_on_finish: bool,
    pub event_loop: StdMutex<Option<PyObject>>,
}

impl AdkLiveSinkConfig {
    pub fn capture_running_loop(&self, py: Python<'_>) {
        if let Ok(mut guard) = self.event_loop.lock() {
            if guard.is_none() {
                if let Ok(asyncio) = py.import("asyncio") {
                    if let Ok(loop_obj) = asyncio.call_method0("get_running_loop") {
                        *guard = Some(loop_obj.into());
                    }
                }
            }
        }
    }

    fn get_event_loop(&self, py: Python<'_>) -> Option<PyObject> {
        self.event_loop
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|obj| obj.clone_ref(py)))
    }

    fn resolve_target_queue(&self, py: Python<'_>, session_key: Option<&str>) -> PyResult<PyObject> {
        if self
            .live_request_queue
            .getattr(py, "send_content")
            .is_ok()
        {
            return Ok(self.live_request_queue.clone_ref(py));
        }

        let key = session_key.unwrap_or("default-session");
        if let Ok(dict) = self.live_request_queue.extract::<&PyDict>(py) {
            if let Some(q) = dict.get_item(key) {
                return Ok(q.into());
            }
        }

        self.live_request_queue.call1(py, (key,))
    }
}

/// Sink component that formats incoming stream items and pushes `Content` into an ADK `LiveRequestQueue`.
#[derive(Clone)]
pub struct AdkLiveSinkComponent<T> {
    config: Arc<AdkLiveSinkConfig>,
    _marker: PhantomData<T>,
}

impl<T> AdkLiveSinkComponent<T> {
    pub fn new(config: Arc<AdkLiveSinkConfig>) -> Self {
        Self {
            config,
            _marker: PhantomData,
        }
    }
    fn close_queue(&self) {
        if self.config.close_on_finish {
            Python::with_gil(|py| {
                if self
                    .config
                    .live_request_queue
                    .getattr(py, "close")
                    .is_ok()
                {
                    let event_loop = self.config.get_event_loop(py);
                    let _ = dispatch_threadsafe_noarg(
                        py,
                        event_loop.as_ref(),
                        &self.config.live_request_queue,
                        "close",
                    );
                }
            });
        }
    }
}

/// Convert a Python payload into a formatted prompt string.
pub trait PyAdkPrompt: Clone + Send + Sync + 'static {
    fn to_prompt(&self, prefix: Option<&str>, separator: &str) -> Option<String>;
    fn to_py_object(&self, py: Python<'_>) -> PyObject;
}

impl PyAdkPrompt for String {
    fn to_prompt(&self, prefix: Option<&str>, separator: &str) -> Option<String> {
        AdkInput::format_prompt(self, prefix, separator)
    }

    fn to_py_object(&self, py: Python<'_>) -> PyObject {
        self.clone().into_py(py)
    }
}

impl PyAdkPrompt for Vec<String> {
    fn to_prompt(&self, prefix: Option<&str>, separator: &str) -> Option<String> {
        AdkInput::format_prompt(self, prefix, separator)
    }

    fn to_py_object(&self, py: Python<'_>) -> PyObject {
        self.clone().into_py(py)
    }
}

impl PyAdkPrompt for PyObject {
    fn to_prompt(&self, prefix: Option<&str>, separator: &str) -> Option<String> {
        Python::with_gil(|py| {
            if let Ok(items) = self.extract::<Vec<String>>(py) {
                return AdkInput::format_prompt(&items, prefix, separator);
            }
            if let Ok(text) = self.extract::<String>(py) {
                return AdkInput::format_prompt(&text, prefix, separator);
            }
            let text = self
                .as_ref(py)
                .str()
                .ok()
                .and_then(|s| s.to_str().ok().map(ToOwned::to_owned))?;
            AdkInput::format_prompt(&text, prefix, separator)
        })
    }

    fn to_py_object(&self, py: Python<'_>) -> PyObject {
        self.clone_ref(py)
    }
}

impl<T: PyAdkPrompt> Sink for AdkLiveSinkComponent<T> {
    type Input = T;

    async fn run(&self, mut input: Receiver<Self::Input>) -> Result<()> {
        while let Some(msg) = input.recv().await {
            let Some(prompt) = msg.payload.to_prompt(
                self.config.prompt_prefix.as_deref(),
                &self.config.separator,
            ) else {
                continue;
            };

            let send_res: PyResult<()> = Python::with_gil(|py| {
                let session_key = if let Some(key_fn) = &self.config.session_key_fn {
                    let arg = msg.payload.to_py_object(py);
                    let res = key_fn
                        .call1(py, (arg.clone_ref(py), msg.source_id.as_deref()))
                        .or_else(|_| key_fn.call1(py, (arg,)))?;
                    Some(res.extract::<String>(py)?)
                } else {
                    msg.source_id.as_deref().map(ToOwned::to_owned)
                };

                let queue = self
                    .config
                    .resolve_target_queue(py, session_key.as_deref())?;
                let event_loop = self.config.get_event_loop(py);
                dispatch_send_content(
                    py,
                    event_loop.as_ref(),
                    &self.config.push_fn,
                    &queue,
                    &self.config.role,
                    &prompt,
                )
            });

            if let Err(err) = send_res {
                self.close_queue();
                return Err(Error::Custom(format!(
                    "AdkLiveSink failed to send content: {}",
                    err
                )));
            }
        }

        self.close_queue();
        Ok(())
    }
}

impl_sink_component!(AdkLiveSinkComponent<String>, String);
impl_sink_component!(AdkLiveSinkComponent<Vec<String>>, Vec<String>);
impl_sink_component!(AdkLiveSinkComponent<PyObject>, PyObject);

/// Python sink that pushes `floq` stream events or windowed batches into a Google ADK `LiveRequestQueue`.
#[pyclass(name = "AdkLiveSink")]
#[derive(Clone)]
pub struct PyAdkLiveSink {
    config: Arc<AdkLiveSinkConfig>,
    string_component: AdkLiveSinkComponent<String>,
    string_stage: PipelineTask<String, ()>,
    vec_component: AdkLiveSinkComponent<Vec<String>>,
    vec_stage: PipelineTask<Vec<String>, ()>,
    py_component: AdkLiveSinkComponent<PyObject>,
    py_stage: PipelineTask<PyObject, ()>,
    rooted: Option<PipelineTask<(), ()>>,
}

impl PyPipelineWrapper<String, ()> for PyAdkLiveSink {
    type Component = AdkLiveSinkComponent<String>;

    fn get_stage(&self) -> &PipelineTask<String, ()> {
        &self.string_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let config = Arc::clone(&component.config);
        let vec_component = AdkLiveSinkComponent::<Vec<String>>::new(Arc::clone(&config));
        let py_component = AdkLiveSinkComponent::<PyObject>::new(Arc::clone(&config));
        Self {
            config,
            string_component: component,
            string_stage: stage,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.string_component
    }
}

impl PyPipelineWrapper<Vec<String>, ()> for PyAdkLiveSink {
    type Component = AdkLiveSinkComponent<Vec<String>>;

    fn get_stage(&self) -> &PipelineTask<Vec<String>, ()> {
        &self.vec_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<Vec<String>, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let config = Arc::clone(&component.config);
        let string_component = AdkLiveSinkComponent::<String>::new(Arc::clone(&config));
        let py_component = AdkLiveSinkComponent::<PyObject>::new(Arc::clone(&config));
        Self {
            config,
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_component: component,
            vec_stage: stage,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.vec_component
    }
}

impl PyPipelineWrapper<PyObject, ()> for PyAdkLiveSink {
    type Component = AdkLiveSinkComponent<PyObject>;

    fn get_stage(&self) -> &PipelineTask<PyObject, ()> {
        &self.py_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), ()>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<PyObject, ()>,
        rooted: Option<PipelineTask<(), ()>>,
    ) -> Self {
        let config = Arc::clone(&component.config);
        let string_component = AdkLiveSinkComponent::<String>::new(Arc::clone(&config));
        let vec_component = AdkLiveSinkComponent::<Vec<String>>::new(Arc::clone(&config));
        Self {
            config,
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_component: component,
            py_stage: stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.py_component
    }
}

#[pymethods]
impl PyAdkLiveSink {
    #[new]
    #[pyo3(signature = (
        live_request_queue,
        prompt_prefix = None,
        separator = "\n---\n".to_string(),
        role = "user".to_string(),
        session_key_fn = None,
        close_on_finish = true,
        event_loop = None
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        live_request_queue: PyObject,
        prompt_prefix: Option<String>,
        separator: String,
        role: String,
        session_key_fn: Option<PyObject>,
        close_on_finish: bool,
        event_loop: Option<PyObject>,
    ) -> PyResult<Self> {
        let push_fn = create_push_helper(py)?;
        let config = Arc::new(AdkLiveSinkConfig {
            live_request_queue,
            push_fn,
            prompt_prefix,
            separator,
            role,
            session_key_fn,
            close_on_finish,
            event_loop: StdMutex::new(event_loop),
        });
        config.capture_running_loop(py);

        let string_component = AdkLiveSinkComponent::<String>::new(Arc::clone(&config));
        let vec_component = AdkLiveSinkComponent::<Vec<String>>::new(Arc::clone(&config));
        let py_component = AdkLiveSinkComponent::<PyObject>::new(Arc::clone(&config));

        Ok(Self {
            config,
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            py_stage: PipelineTask::new(py_component.clone()),
            py_component,
            rooted: None,
        })
    }

    fn __or__(&self, _other: PyObject) -> PyResult<PyObject> {
        Err(PyRuntimeError::new_err(
            "Cannot connect: AdkLiveSink is a terminal component",
        ))
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        self.config.capture_running_loop(py);
        <Self as PyPipelineWrapper<PyObject, ()>>::run_impl(self, py)
    }
}

/// Execution mode for [`PyAdkAgentComponent`]: either a native Rust [`AdkAgent`] HTTP client
/// or an in-process Python callable / runner.
#[derive(Clone)]
pub enum PyAdkBackend<I: AdkInput> {
    Http(AdkAgent<I>),
    Python {
        handler: PyObject,
        user_id: String,
        session_id: String,
        session_key_fn: Option<PyObject>,
        use_source_id: bool,
        ephemeral: bool,
        prompt_prefix: Option<String>,
        separator: String,
    },
}

/// Shared configuration for [`PyAdkAgentComponent`] across `String` and `Vec<String>` inputs.
#[derive(Clone)]
pub struct PyAdkAgentShared {
    pub string_backend: PyAdkBackend<String>,
    pub vec_backend: PyAdkBackend<Vec<String>>,
}

/// Polymorphic ADK agent operator for Python pipelines (`String -> String` and `Vec<String> -> String`).
#[derive(Clone)]
pub struct PyAdkAgentComponent<I: AdkInput> {
    backend: PyAdkBackend<I>,
    shared: Arc<PyAdkAgentShared>,
}

impl<I> PyAdkAgentComponent<I>
where
    I: AdkInput + Clone + IntoPy<PyObject>,
{
    pub fn new(backend: PyAdkBackend<I>, shared: Arc<PyAdkAgentShared>) -> Self {
        Self { backend, shared }
    }

    fn resolve_py_session(
        msg: &Message<I>,
        default_session_id: &str,
        session_key_fn: Option<&PyObject>,
        use_source_id: bool,
        ephemeral: bool,
    ) -> String {
        if ephemeral {
            let n = PY_EPHEMERAL_COUNTER.fetch_add(1, Ordering::Relaxed);
            return format!("floq-ephemeral-{}-{}", msg.event_timestamp, n);
        }
        if let Some(key_fn) = session_key_fn {
            let extracted = Python::with_gil(|py| {
                let arg = msg.payload.clone().into_py(py);
                key_fn
                    .call1(py, (arg.clone_ref(py), msg.source_id.as_deref()))
                    .or_else(|_| key_fn.call1(py, (arg,)))
                    .and_then(|res| res.extract::<String>(py))
                    .ok()
            });
            if let Some(sid) = extracted {
                return sid;
            }
        }
        if use_source_id {
            if let Some(source_id) = msg.source_id.as_deref() {
                if !source_id.is_empty() {
                    return source_id.to_string();
                }
            }
        }
        default_session_id.to_string()
    }
}

impl<I> Operator for PyAdkAgentComponent<I>
where
    I: AdkInput + Clone + IntoPy<PyObject>,
{
    type Input = I;
    type Output = String;

    async fn run(&self, mut input: Receiver<Self::Input>, output: Sender<Self::Output>) -> Result<()> {
        match &self.backend {
            PyAdkBackend::Http(agent) => Operator::run(agent, input, output).await,
            PyAdkBackend::Python {
                handler,
                user_id,
                session_id,
                session_key_fn,
                use_source_id,
                ephemeral,
                prompt_prefix,
                separator,
            } => {
                while let Some(msg) = input.recv().await {
                    let Some(prompt) =
                        msg.payload
                            .format_prompt(prompt_prefix.as_deref(), separator)
                    else {
                        continue;
                    };

                    let resolved_session = Self::resolve_py_session(
                        &msg,
                        session_id,
                        session_key_fn.as_ref(),
                        *use_source_id,
                        *ephemeral,
                    );

                    let response_res: PyResult<String> = Python::with_gil(|py| {
                        let res = handler
                            .call1(py, (&prompt, &resolved_session, user_id.as_str()))
                            .or_else(|_| handler.call1(py, (&prompt, &resolved_session)))
                            .or_else(|_| handler.call1(py, (&prompt,)))?;
                        res.extract::<String>(py)
                    });

                    let reply = response_res.map_err(|e| {
                        Error::Custom(format!("AdkAgent Python handler failed: {}", e))
                    })?;

                    if !reply.is_empty() {
                        let out_msg = msg.map(|_| reply);
                        if output.send(out_msg).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

impl_operator_component!(PyAdkAgentComponent<String>, String, String);
impl_operator_component!(PyAdkAgentComponent<Vec<String>>, Vec<String>, String);

/// Python wrapper for the polymorphic `AdkAgent` operator.
///
/// Accepts either `String` or `Vec<String>` upstream and emits the agent's `String` response downstream:
/// `source | window | adk_agent | collector` or `source | filter | adk_agent | stream`.
#[pyclass(name = "AdkAgent")]
#[derive(Clone)]
pub struct PyAdkAgent {
    string_component: PyAdkAgentComponent<String>,
    string_stage: PipelineTask<String, String>,
    vec_component: PyAdkAgentComponent<Vec<String>>,
    vec_stage: PipelineTask<Vec<String>, String>,
    rooted: Option<PipelineTask<(), String>>,
}

impl PyPipelineWrapper<String, String> for PyAdkAgent {
    type Component = PyAdkAgentComponent<String>;

    fn get_stage(&self) -> &PipelineTask<String, String> {
        &self.string_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), String>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<String, String>,
        rooted: Option<PipelineTask<(), String>>,
    ) -> Self {
        let shared = Arc::clone(&component.shared);
        let vec_component = PyAdkAgentComponent::new(shared.vec_backend.clone(), shared);
        Self {
            string_component: component,
            string_stage: stage,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.string_component
    }
}

impl PyPipelineWrapper<Vec<String>, String> for PyAdkAgent {
    type Component = PyAdkAgentComponent<Vec<String>>;

    fn get_stage(&self) -> &PipelineTask<Vec<String>, String> {
        &self.vec_stage
    }

    fn get_rooted(&self) -> Option<&PipelineTask<(), String>> {
        self.rooted.as_ref()
    }

    fn from_parts(
        component: Self::Component,
        stage: PipelineTask<Vec<String>, String>,
        rooted: Option<PipelineTask<(), String>>,
    ) -> Self {
        let shared = Arc::clone(&component.shared);
        let string_component = PyAdkAgentComponent::new(shared.string_backend.clone(), shared);
        Self {
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_component: component,
            vec_stage: stage,
            rooted,
        }
    }

    fn get_component(&self) -> &Self::Component {
        &self.vec_component
    }
}

fn build_rust_session_mode<I>(
    session_id: &str,
    session_key_fn: Option<PyObject>,
    use_source_id: bool,
    ephemeral: bool,
) -> AdkSessionMode<I>
where
    I: AdkInput + Clone + IntoPy<PyObject>,
{
    if ephemeral {
        return AdkSessionMode::Ephemeral {
            prefix: "floq_ephemeral".to_string(),
        };
    }
    if let Some(key_fn) = session_key_fn {
        return AdkSessionMode::Keyed(Arc::new(move |payload: &I| {
            Python::with_gil(|py| {
                let arg = payload.clone().into_py(py);
                key_fn
                    .call1(py, (arg,))
                    .and_then(|res| res.extract::<String>(py))
                    .unwrap_or_else(|_| "default-session".to_string())
            })
        }));
    }
    if use_source_id {
        return AdkSessionMode::FromSourceId {
            fallback: session_id.to_string(),
        };
    }
    AdkSessionMode::Shared(session_id.to_string())
}

#[pymethods]
impl PyAdkAgent {
    #[new]
    #[pyo3(signature = (
        *,
        base_url = None,
        app_name = None,
        handler = None,
        user_id = "floq".to_string(),
        session_id = "default-session".to_string(),
        session_key_fn = None,
        use_source_id = false,
        ephemeral = false,
        prompt_prefix = None,
        separator = "\n---\n".to_string()
    ))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python<'_>,
        base_url: Option<String>,
        app_name: Option<String>,
        handler: Option<PyObject>,
        user_id: String,
        session_id: String,
        session_key_fn: Option<PyObject>,
        use_source_id: bool,
        ephemeral: bool,
        prompt_prefix: Option<String>,
        separator: String,
    ) -> PyResult<Self> {
        let (string_backend, vec_backend) = match (base_url, app_name, handler) {
            (Some(url), Some(app), None) => {
                let str_mode = build_rust_session_mode::<String>(
                    &session_id,
                    session_key_fn.as_ref().map(|f| f.clone_ref(py)),
                    use_source_id,
                    ephemeral,
                );
                let vec_mode = build_rust_session_mode::<Vec<String>>(
                    &session_id,
                    session_key_fn,
                    use_source_id,
                    ephemeral,
                );

                let mut str_agent = AdkAgent::<String>::new(url.clone(), app.clone())
                    .with_user_id(user_id.clone())
                    .with_session_mode(str_mode)
                    .with_separator(separator.clone());
                let mut vec_agent = AdkAgent::<Vec<String>>::new(url, app)
                    .with_user_id(user_id)
                    .with_session_mode(vec_mode)
                    .with_separator(separator);

                if let Some(prefix) = prompt_prefix {
                    str_agent = str_agent.with_prompt_prefix(prefix.clone());
                    vec_agent = vec_agent.with_prompt_prefix(prefix);
                }

                (PyAdkBackend::Http(str_agent), PyAdkBackend::Http(vec_agent))
            }
            (None, None, Some(py_handler)) => (
                PyAdkBackend::Python {
                    handler: py_handler.clone_ref(py),
                    user_id: user_id.clone(),
                    session_id: session_id.clone(),
                    session_key_fn: session_key_fn.as_ref().map(|f| f.clone_ref(py)),
                    use_source_id,
                    ephemeral,
                    prompt_prefix: prompt_prefix.clone(),
                    separator: separator.clone(),
                },
                PyAdkBackend::Python {
                    handler: py_handler,
                    user_id,
                    session_id,
                    session_key_fn,
                    use_source_id,
                    ephemeral,
                    prompt_prefix,
                    separator,
                },
            ),
            _ => {
                return Err(PyValueError::new_err(
                    "AdkAgent requires either (`base_url` and `app_name`) or `handler`",
                ));
            }
        };

        let shared = Arc::new(PyAdkAgentShared {
            string_backend: string_backend.clone(),
            vec_backend: vec_backend.clone(),
        });
        let string_component = PyAdkAgentComponent::new(string_backend, Arc::clone(&shared));
        let vec_component = PyAdkAgentComponent::new(vec_backend, shared);

        Ok(Self {
            string_stage: PipelineTask::new(string_component.clone()),
            string_component,
            vec_stage: PipelineTask::new(vec_component.clone()),
            vec_component,
            rooted: None,
        })
    }

    fn __or__(&self, other: PyObject) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            <Self as PyPipelineWrapper<String, String>>::compose_string(self, py, &other)
        })
    }

    fn run<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        <Self as PyPipelineWrapper<String, String>>::run_impl(self, py)
    }
}
