
# eharbor

`eharbor` is an innovative Erlang library designed to enhance the resilience and efficiency of distributed systems under heavy load. Unlike traditional approaches that rely solely on queues, `eharbor` intelligently combines load-shedding, request deduplication, and concurrency control to provide a more robust solution to system overload.

## Key Features

- **Intelligent Load-Shedding**: Goes beyond simple queuing to proactively manage system load, preventing overload before it occurs.
- **Request Deduplication**: Efficiently handles multiple identical requests by processing a single instance and sharing the result, significantly reducing redundant workload.
- **Concurrency Management**: Limits the number of concurrent operations, protecting backend systems (like databases) from being overwhelmed.
- **Configurable Buffering**: Offers fine-tuned control over buffer sizes and system limits, ensuring optimal performance tailored to specific operational needs.

## Why eharbor?

Traditional overload protection mechanisms often rely heavily on queuing requests. However, queues alone cannot prevent system overload; they merely postpone the problem. `eharbor` addresses this by implementing an intelligent system that manages queued requests, actively reduces unnecessary workload, and controls the concurrency of task processing.

## Installation

To install `eharbor`, add it to your `rebar.config` dependencies:

```erlang
{deps, [
    {eharbor, "1.0.0"}
]}.
```

Then run `rebar3` to fetch and compile the library:

```shell
$ rebar3 compile
```

## Getting Started

### Configuration

First, set up your `eharbor` configuration:

```erlang

%% Sample configuration
Config = #{
  name => my_harbor,
  backlog => 500,  %% Buffer size
  piers => 50,     %% Concurrency limit
  breakwater_limit => 1000,  %% Max waiting processes
  dedup => true,   %% Enable request deduplication
  ordered => true  %% Maintain operation order
}.
```

### Launching eharbor

Start an instance of `eharbor` with your configuration or add it to your supervisor tree:

```erlang
{ok, _Pid} = eharbor:start_link(Config).
```

### Running Tasks

Use `eharbor` to run tasks efficiently:

```erlang

%% Execute the task through `eharbor`
Result = eharbor:run(fun some_mod:slow_function/1, [Param], Config).
```

## Examples

### Basic Usage

```erlang
%% Start eharbor
{ok, _Pid} = eharbor:start_link(Config).

%% Run the function via eharbor
Result = eharbor:run(fun some_mod:slow_function/1, [Arg], Config).
```

### Ignoring Parameters

Some parameters sometimes need to be excluded from deduplication matching.

```erlang
%% Start eharbor
{ok, _Pid} = eharbor:start_link(Config).

%% Define a resource-intensive function
SourceIP = ...
IntensiveFun = fun(Arg) -> some_mod:heavy_computation(Arg, SourceIP) end.

%% Run the function via eharbor
Result = eharbor:run(IntensiveFun, [Arg], Config).
```

## Documentation

For detailed documentation, including advanced configurations and operational guidelines, please refer to our repository's `doc` directory or visit our [online documentation](https://github.com/loopsocial/eharbor/doc).

## Contributing

Contributions to `eharbor` are welcome! Please feel free to contribute, whether it's feature requests, bug reports, or code contributions. For code contributions, please fork the repository and submit a pull request with your changes.

## License

`eharbor` is released under the Apache License 2.0. Please look at the [LICENSE](LICENSE) file for more details.
