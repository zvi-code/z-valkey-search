# Developer Guide


## Build

Valkey-search is built using the Bazel build system.

### Installing Bazelisk as Bazel

It is recommended to use [Bazelisk](https://github.com/bazelbuild/bazelisk) installed as `bazel`, to avoid Bazel compatibility issues.

On Linux, run the following commands:

```console
sudo wget -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-$([ $(uname -m) = "aarch64" ] && echo "arm64" || echo "amd64")
sudo chmod +x /usr/local/bin/bazel
```

Add /usr/local/bin to your path:

```console
grep -q 'export PATH=.*:/usr/local/bin' ~/.bashrc || echo 'export PATH=$PATH:/usr/local/bin' >> ~/.bashrc
source ~/.bashrc
```

### Supported Compiler Versions

Known supported versions:
- GCC >= 12.2.0
- Clang >= 16.0.6


If you want to use clang, run the following:
```console
echo "build --config=clang" > .bazelrc_local
```

## Testing with Bazel

All the tests can be built and run with:

```console
bazel test //testing/...
```

An individual test target can be run with a more specific Bazel [label](https://bazel.build/versions/master/docs/build-ref.html#Labels), e.g. to build and run only
the units tests in [testing/ft_search_test.cc](https://github.com/valkey-io/valkey-search/blob/main/testing/ft_search_test.cc):

```console
bazel test //testing:ft_search_test
```

To observe more verbose test output:

```console
bazel test --test_output=streamed //testing:ft_search_test
```

It's also possible to pass into a test additional command-line args via `--test_arg`. For
example, for extremely verbose test debugging:

```console
bazel test --test_output=streamed //testing:ft_search_test --test_arg="-l trace"
```

By default, tests are run with the [gperftools](https://github.com/gperftools/gperftools) heap
checker enabled in "normal" mode to detect leaks. For other mode options, see the gperftools
heap checker [documentation](https://gperftools.github.io/gperftools/heap_checker.html). To
disable the heap checker or change the mode, set the HEAPCHECK environment variable:

```
# Disables the heap checker
bazel test //testing/... --test_env=HEAPCHECK=
# Changes the heap checker to "minimal" mode
bazel test //testing/... --test_env=HEAPCHECK=minimal
```

Bazel will by default cache successful test results. To force it to rerun tests:

```
bazel test  //testing:ft_search_test --cache_test_results=no
```

Bazel will by default run all tests inside a sandbox, which disallows access to the
local filesystem. If you need to break out of the sandbox (for example to run under a
local script or tool with [`--run_under`](https://docs.bazel.build/versions/master/user-manual.html#flag--run_under)),
you can run the test with `--strategy=TestRunner=local`, e.g.:

```
bazel test //testing:ft_search_test --strategy=TestRunner=local --run_under=/some/path/foobar.sh
```

