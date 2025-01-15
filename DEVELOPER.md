# Developer Guide


## Build

ValkeySearch is built using the Bazel build system.

### Installing Bazelisk as Bazel

It is recommended to use [Bazelisk](https://github.com/bazelbuild/bazelisk) installed as `bazel`, to avoid Bazel compatibility issues.

On Linux, run the following commands:

```bash
sudo wget -O /usr/local/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-$([ $(uname -m) = "aarch64" ] && echo "arm64" || echo "amd64")
sudo chmod +x /usr/local/bin/bazel
```

Add /usr/local/bin to your path:

```bash
grep -q 'export PATH=.*:/usr/local/bin' ~/.bashrc || echo 'export PATH=$PATH:/usr/local/bin' >> ~/.bashrc
source ~/.bashrc
```

Build requires `gcc/g++` version 11 or higher, and `glibc` to be available on the host. On a Debian-based distribution, run the following commands:

```bash
sudo apt update
sudo apt upgrade
sudo apt install libc6-dev gcc g++

# If your gcc version is below 11, continue with the following:
# Add PPA to be able to newer gcc/g++ versions
sudo add-apt-repository ppa:ubuntu-toolchain-r/test
sudo apt update

# Install gcc-11 and g++-11
sudo apt install gcc-11 g++-11

# Set gcc-11 and g++-11 as the default compilers
sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-11 100
sudo update-alternatives --install /usr/bin/g++ g++ /usr/bin/g++-11 100
```

### Toolchain


Bazel is setup to download and use llvm toolchain automatically.


To switch to the host compiler, comment out the `register_toolchains` line 
in [MODULE.bazel](https://github.com/valkey-io/valkey-search/blob/main/MODULE.bazel#L29).
Both GCC and Clang are supported, with GCC set as the default.


If you are using Clang, run the following prior to invoking the build:


```bash
echo "build --config=clang" > .bazelrc_local
```


Known supported versions:
- GCC >= 12.2.0
- Clang >= 16.0.6


### Building the Module

To perform a non-optimized build, use the following command:

```bash
bazel build //src:valkeysearch
```

For an optimized binary, suitable for production or benchmarking, use:

```bash
bazel build -c opt config=lto //src:valkeysearch
```

To include the symbol table:

```bash
bazel build --copt=-g --linkopt=-g --strip=never //src:valkeysearch
```

## Testing

All the tests can be built and run with:

```bash
bazel test //testing/...
```

An individual test target can be run with a more specific Bazel [label](https://bazel.build/versions/master/docs/build-ref.html#Labels), e.g. to build and run only
the units tests in [testing/ft_search_test.cc](https://github.com/valkey-io/valkey-search/blob/main/testing/ft_search_test.cc):

```bash
bazel test //testing:ft_search_test
```

To observe more verbose test output:

```bash
bazel test --test_output=streamed //testing:ft_search_test
```

It's also possible to pass into a test additional command-line args via `--test_arg`. For
example, for extremely verbose test debugging:

```bash
bazel test --test_output=streamed //testing:ft_search_test --test_arg="-l trace"
```

By default, tests are run with the [gperftools](https://github.com/gperftools/gperftools) heap
checker enabled in "normal" mode to detect leaks. For other mode options, see the gperftools
heap checker [documentation](https://gperftools.github.io/gperftools/heap_checker.html). To
disable the heap checker or change the mode, set the HEAPCHECK environment variable:

```bash
# Disables the heap checker
bazel test //testing/... --test_env=HEAPCHECK=
# Changes the heap checker to "minimal" mode
bazel test //testing/... --test_env=HEAPCHECK=minimal
```

Bazel will by default cache successful test results. To force it to rerun tests:

```bash
bazel test  //testing:ft_search_test --cache_test_results=no
```

Bazel will by default run all tests inside a sandbox, which disallows access to the
local filesystem. If you need to break out of the sandbox (for example to run under a
local script or tool with [`--run_under`](https://docs.bazel.build/versions/master/user-manual.html#flag--run_under)),
you can run the test with `--strategy=TestRunner=local`, e.g.:

```bash
bazel test //testing:ft_search_test --strategy=TestRunner=local --run_under=/some/path/foobar.sh
```

## Loading

ValkeySearch is compatible with any version of Valkey and can also be loaded into Redis versions 7.0 and 7.2. To load the module, execute the following command:

```bash
/path/to/valkey-server "--loadmodule /path/to/libvalkeysearch.so  --reader-threads 64 --writer-threads 64"
```

For optimal performance, set the --reader-threads and --writer-threads parameters to match the number of vCPUs available on your machine.

## Coding Style/Quality

We follow the [Google coding style](https://google.github.io/styleguide/) and use `clang-tidy` and `clang-format` to enforce code quality and ensure adherence to this style guide.

### Installation

To set up the necessary tools on a Debian-based distribution, follow these steps:


1. Install the LLVM repository and set it up:
```bash
sudo bash -c "$(wget -O - https://apt.llvm.org/llvm.sh)"
```

2. Install clang-tidy and related dependencies for your latest supported version:
```bash
sudo apt install clang-16 clang++-16
sudo update-alternatives --set clang /usr/bin/clang-16
sudo update-alternatives --set clang++ /usr/bin/clang++-16
sudo apt install libc++-16-dev libc++abi-16-dev clang-tidy-16
sudo update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-16 100
```

### Running Clang-Tidy Locally
clang-tidy requires knowledge of the project build configuration to function correctly. To achieve this, follow these steps:

1. Generate the build configuration:
```bash
bazel run @hedron_compile_commands//:refresh_all
```

2. Run `clang-tidy` on a specific file: 
```bash
clang-tidy -p compile_commands.json src/some_file.cc
```


### Convenience Script

A convenient script has been provided to automate the process of running clang-tidy and clang-format on your local changes. You can execute it with:

```bash
ci/check_changes.sh --cached
```

- `--cached` is an optional parameter which direct the script to check the staged files, i.e., the files that have been added to the staging area (via git add) but not yet committed.
