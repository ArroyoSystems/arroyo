// Adapted from https://github.com/risingwavelabs/arrow-udf/tree/main/arrow-udf-python
// Copyright 2024 RisingWave Labs
// Modified in 2024 by Arroyo Systems
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! High-level API for Python sub-interpreters.

#[allow(deprecated)]
use pyo3::GILPool;
use pyo3::{ffi::*, prepare_freethreaded_python, PyErr, Python};
use std::ffi::CStr;

/// A Python sub-interpreter with its own GIL.
#[derive(Debug)]
pub struct SubInterpreter {
    state: *mut PyThreadState,
}

impl SubInterpreter {
    /// Create a new sub-interpreter.
    pub fn new() -> Result<Self, PyError> {
        prepare_freethreaded_python();
        // XXX: import the `decimal` module in the main interpreter before creating sub-interpreters.
        //      otherwise it will cause `SIGABRT: pointer being freed was not allocated`
        //      when importing decimal in the second sub-interpreter.
        Python::with_gil(|py| {
            py.import_bound("decimal").unwrap();
        });

        // reference: https://github.com/PyO3/pyo3/blob/9a36b5078989a7c07a5e880aea3c6da205585ee3/examples/sequential/tests/test.rs
        let config = PyInterpreterConfig {
            use_main_obmalloc: 0,
            allow_fork: 0,
            allow_exec: 0,
            allow_threads: 0,
            allow_daemon_threads: 0,
            check_multi_interp_extensions: 1,
            gil: PyInterpreterConfig_OWN_GIL,
        };
        let mut state: *mut PyThreadState = std::ptr::null_mut();
        // FIXME: according to the documentation:
        // - "the GIL must be held before calling this function"
        // - "a current thread state must be set on entry"
        // but we don't acquire the GIL here.
        let status: PyStatus = unsafe { Py_NewInterpreterFromConfig(&mut state, &config) };
        if unsafe { PyStatus_IsError(status) } == 1 {
            let msg = unsafe { CStr::from_ptr(status.err_msg) };
            return Err(anyhow::anyhow!(
                "failed to create sub-interpreter: {}",
                msg.to_string_lossy()
            )
            .into());
        }
        // release the GIL
        unsafe { PyEval_SaveThread() };
        Ok(Self { state })
    }

    /// Run a closure in the sub-interpreter.
    ///
    /// Please note that if the return value contains any `Py` object (e.g. `PyErr`),
    /// this object must be dropped in this sub-interpreter, otherwise it will cause
    /// `SIGABRT: pointer being freed was not allocated`.
    pub fn with_gil<F, R>(&self, f: F) -> Result<R, PyError>
    where
        F: for<'py> FnOnce(Python<'py>) -> Result<R, PyError>,
    {
        // switch to the sub-interpreter and acquire GIL
        unsafe { PyEval_RestoreThread(self.state) };

        // Safety: the GIL is already held
        // this pool is used to increment the internal GIL count of pyo3.
        #[allow(deprecated)]
        let pool = unsafe { GILPool::new() };
        let ret = f(pool.python());
        drop(pool);

        // release the GIL
        unsafe { PyEval_SaveThread() };
        ret
    }
}

impl Drop for SubInterpreter {
    fn drop(&mut self) {
        unsafe {
            // switch to the sub-interpreter
            PyEval_RestoreThread(self.state);
            // destroy the sub-interpreter
            Py_EndInterpreter(self.state);
        }
    }
}

/// The error type for Python sub-interpreters.
///
/// This type is a wrapper around `anyhow::Error`. The special thing is that
/// when it comes from `PyErr`, only the error message is retained, and the
/// original type is discarded. This is to avoid the problem of `PyErr` being
/// dropped outside the sub-interpreter.
#[derive(Debug)]
pub struct PyError {
    anyhow: anyhow::Error,
}

/// Converting from `PyErr` only keeps the error message.
impl From<PyErr> for PyError {
    fn from(err: PyErr) -> Self {
        Self {
            anyhow: anyhow::anyhow!(err.to_string()),
        }
    }
}

impl From<anyhow::Error> for PyError {
    fn from(err: anyhow::Error) -> Self {
        Self { anyhow: err }
    }
}

impl From<PyError> for anyhow::Error {
    fn from(err: PyError) -> Self {
        err.anyhow
    }
}
