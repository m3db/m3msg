Guideline
=========

This document suggests guidelines for usage of the components provided by this
repository. Components:

(1) `gometalinter`: This [tool] provides wrappers around a variety of static
analysis tools for golang.

[tool]: https://github.com/alecthomas/gometalinter

## gometalinter
`gometalinter` is a tool which wraps about 10-15 other static analysis tools for
golang. They perform a wide range of functions, from checking spelling,
eliminating dead code to optimizing memory layout. The guiding principle when
using these tools is to improve readability & maintainability of the code. Only
use the bits that you find helpful in keeping code quality up, don't be dogmatic
about chasing a 100% correctness.

- Installation/Integration with Makefiles: `common.mk` provides a target to
install the `gometalinter`. Usage:
```sh
# From the command line
$ make -f common.mk install-metalinter

# To use within your own makefile, include `common.mk`,
# and re-use the target as you would normally.
```

- Using `gometalinter`: This repository provides `metalint.sh` to wrap the
underlying functionality.  The easiest way to understand how to use it is to
study how `m3collector` does it -

  - Relevant bits from the `Makefile`

  ```
  metalint_check       := .ci/metalint.sh
  metalint_config      := .metalinter.json
  metalint_exclude     := .excludemetalint

  .PHONY: metalint
  metalint: install-metalinter
    @($(metalint_check) $(metalint_config) $(metalint_exclude) \
      && echo "metalinted successfully!") || (echo "metalinter failed" && exit 1)
  ```

  - Configuration for the underlying linter, `.metalinter.json`

  ```
  { "Enable": [
      "deadcode",
      "varcheck",
      "structcheck",
      "goconst",
      "ineffassign",
      "unconvert",
      "misspell",
      "unparam",
      "megacheck" ],
    "Deadline": "3m",
    "EnableGC": true }
  ```

  - List of exclude dirs, `.excludemetalint`

  ```
  vendor/
  generated/
  _mock.go
  ```

After this, running `make metalint` runs the metalinter and lists applicable
errors (if any). Note: it's suggested to include this target in the Make target
used by `.travis.yaml` to ensure the metalinter is run by the CI process too.

<details>
<summary>Advanced configuration: customising linter arguments</summary>
There are instances where you might need to configure the commands run by the
sub-linter. For instance, `m3db` needs to specify build tags for it's
integration test suite. Here's the `.metalinter.json` configuration it uses for
the same:

  ```
  { "Linters": {
      "unused":   "unused -tags integration:PATH:LINE:COL:MESSAGE",
      "gosimple": "gosimple -tags integration:PATH:LINE:COL:MESSAGE" },
    "Enable":
      [ "deadcode"
      , "varcheck"
      , "structcheck"
      , "goconst"
      , "ineffassign"
      , "unconvert"
      , "misspell"
      , "gosimple"
      , "unused"  ],
    "Deadline": "3m",
    "EnableGC": true }
  ```

</details>

### Q&A
- Which linters to enable?
`gometalinter` integrates with 10-15 linters, some which share concerns. I
suggest enabling them one at a time, and work through the issues raised by each.

In some cases, you'll find there's too much noise (especially with `dupl`, and
`gocyclo`). Skim the issues raised and if you don't find anything useful, skip
enabling that particular linter all together.

In other cases, you may find there are certain errors (especially default value
for enums) which are raised as false positives. For those cases, you'll have to
suppress the warnings using comments. More on this in a section below.

### Suppressing Warnings
`gometalinter` supports suppression of linter messages via comment directives.
The form of the directive is:

```
// nolint[: <linter>[, <linter>, ...]]
```

Suppression works in the following way:

1. Line-level suppression

    A comment directive suppresses any linter messages on that line.

    eg. In this example any messages for `a := 10` will be suppressed and errcheck
    messages for `defer r.Close()` will also be suppressed.

    ```go
    a := 10 // nolint
    a = 2
    defer r.Close() // nolint: errcheck
    ```

2. Statement-level suppression

    A comment directive at the same indentation level as a statement it
    immediately precedes will also suppress any linter messages in that entire
    statement.

    eg. In this example all messages for `SomeFunc()` will be suppressed.

    ```go
    // nolint
    func SomeFunc() {
    }
    ```

3. File-level suppression

    A comment directive preceding a package directive will also suppress any
    linter messages in that entire file (note: it only suppresses warnings in
    that file, not in all files on that package).

    eg. In this example all messages for the file will be suppressed.

    ```go
    // nolint
    package example
    ```
