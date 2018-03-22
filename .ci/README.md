ci-scripts
==========

This repository contains scripts used by sibling repositories for builds. It's
intended to be used as a submodule, conventionally stored as a folder named
`.ci` at the root level.

Repositories using this package are expected to the following conventions:
  - Makefile in root level of the package
  - make targets for binary built
  - make targets suffixed with `-linux-amd64` for each binary target
  - make creates builds under the same name as the make targets under a new
    sub-folder: `bin`
  - make target 'all' runs all tests, and builds all binaries

Consult [GUIDELINE.md] for more detailed usage information.

[GUIDELINE.md]: https://github.com/m3db/ci-scripts/blob/master/GUIDELINE.md
