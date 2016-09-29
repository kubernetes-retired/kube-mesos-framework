# Copyright 2016 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

DBG_MAKEFILE ?=
ifeq ($(DBG_MAKEFILE),1)
    $(warning ***** starting Makefile for goal(s) "$(MAKECMDGOALS)")
    $(warning ***** $(shell date))
else
    # If we're not debugging the Makefile, don't echo recipes.
    MAKEFLAGS += -s
endif


# Old-skool build tools.
#
# Commonly used targets (see each target for more information):
#   all: Build code.
#   test: Run tests.
#   clean: Clean up.

# It's necessary to set this because some environments don't link sh -> bash.
SHELL := /bin/bash

# We don't need make's built-in rules.
MAKEFLAGS += --no-builtin-rules
.SUFFIXES:

# Constants used throughout.
.EXPORT_ALL_VARIABLES:
OUT_DIR ?= _output
BIN_DIR := $(OUT_DIR)/bin
PRJ_SRC_PATH := k8s.io/kubernetes
GENERATED_FILE_PREFIX := zz_generated.

# Metadata for driving the build lives here.
META_DIR := .make

# Our build flags.
# TODO(thockin): it would be nice to just use the native flags.  Can we EOL
#                these "wrapper" flags?
KUBE_GOFLAGS := $(GOFLAGS)
KUBE_GOLDFLAGS := $(GOLDFLAGS)
KUBE_GOGCFLAGS = $(GOGCFLAGS)

# This controls the verbosity of the build.  Higher numbers mean more output.
KUBE_VERBOSE ?= 1

# Build code.
#
# Args:
#   WHAT: Directory names to build.  If any of these directories has a 'main'
#     package, the build will produce executable files under $(OUT_DIR)/go/bin.
#     If not specified, "everything" will be built.
#   GOFLAGS: Extra flags to pass to 'go' when building.
#   GOLDFLAGS: Extra linking flags passed to 'go' when building.
#   GOGCFLAGS: Additional go compile flags passed to 'go' when building.
#
# Example:
#   make
#   make all
#   make all WHAT=cmd/kubelet GOFLAGS=-v
#   make all GOGCFLAGS="-N -l"
#     Note: Use the -N -l options to disable compiler optimizations an inlining.
#           Using these build options allows you to subsequently use source
#           debugging tools like delve.
.PHONY: all
all:
	hack/make-rules/build.sh $(WHAT)

# Runs all the presubmission verifications.
#
# Args:
#   BRANCH: Branch to be passed to verify-godeps.sh script.
#
# Example:
#   make verify
#   make verify BRANCH=branch_x
.PHONY: verify
verify:
	KUBE_VERIFY_GIT_BRANCH=$(BRANCH) hack/make-rules/verify.sh -v
	hack/make-rules/vet.sh

# Build and run tests.
#
# Args:
#   WHAT: Directory names to test.  All *_test.go files under these
#     directories will be run.  If not specified, "everything" will be tested.
#   TESTS: Same as WHAT.
#   GOFLAGS: Extra flags to pass to 'go' when building.
#   GOLDFLAGS: Extra linking flags to pass to 'go' when building.
#   GOGCFLAGS: Additional go compile flags passed to 'go' when building.
#
# Example:
#   make check
#   make test
#   make check WHAT=pkg/kubelet GOFLAGS=-v
.PHONY: check test
check test: 
	hack/make-rules/test.sh $(WHAT) $(TESTS)

# Build and run cmdline tests.
#
# Example:
#   make test-cmd
.PHONY: test-cmd
test-cmd:
	hack/make-rules/test-cmd.sh

# Remove all build artifacts.
#
# Example:
#   make clean
#
.PHONY: clean
clean: clean_meta
	build/make-clean.sh
	rm -rf $(OUT_DIR)
	rm -rf Godeps/_workspace # Just until we are sure it is gone

# Remove make-related metadata files.
#
# Example:
#   make clean_meta
.PHONY: clean_meta
clean_meta:
	rm -rf $(META_DIR)


# Run 'go vet'.
#
# Args:
#   WHAT: Directory names to vet.  All *.go files under these
#     directories will be vetted.  If not specified, "everything" will be
#     vetted.
#
# Example:
#   make vet
#   make vet WHAT=pkg/kubelet
.PHONY: vet
vet:
	hack/make-rules/vet.sh $(WHAT)

# Build a release
#
# Example:
#   make release
.PHONY: release
release:
	build/release.sh

# Build a release, but skip tests
#
# Example:
#   make release-skip-tests
.PHONY: release-skip-tests quick-release
release-skip-tests quick-release:
	KUBE_RELEASE_RUN_TESTS=n KUBE_FASTBUILD=true build/release.sh

# Cross-compile for all platforms
#
# Example:
#   make cross
.PHONY: cross
cross:
	hack/make-rules/cross.sh
