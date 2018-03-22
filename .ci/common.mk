metalinter_version   := v2.0.0
badtime_version      := a1d80fa39058e2de323bf0b54d47bfab92e9a97f
mockclean_version    := 3e9c30b229f100027d742104ad6d6b2d968374bd
coverfile            := cover.out
coverage_xml         := coverage.xml
coverage_html        := coverage.html
junit_xml            := junit.xml
convert_test_data    := .ci/convert-test-data.sh
test                 := .ci/test-cover.sh
test_one_integration := .ci/test-one-integration.sh
test_ci_integration  := .ci/test-integration.sh
test_log             := test.log

install-vendor: install-glide
	@echo Installing glide deps
	glide --debug install

install-glide:
		@which glide > /dev/null || (go get -u github.com/Masterminds/glide && cd $(GOPATH)/src/github.com/Masterminds/glide && git checkout v0.12.3 && go install)
		@glide -version > /dev/null || (echo "Glide install failed" && exit 1)

install-ci:
	make install-vendor

install-metalinter:
	@which gometalinter > /dev/null || (go get -u github.com/alecthomas/gometalinter && \
		cd $(GOPATH)/src/github.com/alecthomas/gometalinter && \
		git checkout $(metalinter_version) && \
		go install && gometalinter --install)
	@which gometalinter > /dev/null || (echo "gometalinter install failed" && exit 1)

install-linter-badtime:
	@which badtime > /dev/null || (go get -u github.com/m3db/build-tools/linters/badtime && \
		cd $(GOPATH)/src/github.com/m3db/build-tools/linters/badtime && \
		git checkout $(badtime_version) && go install)
	@which badtime > /dev/null || (echo "badtime install failed" && exit 1)

install-util-mockclean:
	@which mockclean > /dev/null || (go get -u github.com/m3db/build-tools/utilities/mockclean && \
		cd $(GOPATH)/src/github.com/m3db/build-tools/utilities/mockclean && \
		git checkout $(mockclean_version) && go install)
	@which mockclean > /dev/null || (echo "mockclean install failed" && exit 1)

test-base:
	@which go-junit-report > /dev/null || go get -u github.com/sectioneight/go-junit-report
	$(test) $(coverfile) | tee $(test_log)

test-base-xml: test-base
	go-junit-report < $(test_log) > $(junit_xml)
	gocov convert $(coverfile) | gocov-xml > $(coverage_xml)
	@$(convert_test_data) $(coverage_xml)
	@rm $(coverfile) &> /dev/null

test-base-html: test-base
	gocov convert $(coverfile) | gocov-html > $(coverage_html) && (which open && open $(coverage_html))
	@rm -f $(test_log) &> /dev/null

test-base-integration:
	go test -v -tags=integration ./integration

# Usage: make test-base-single-integration name=<test_name>
test-base-single-integration:
	$(test_one_integration) $(name)

test-base-ci-unit: test-base
	@which goveralls > /dev/null || go get -u -f github.com/m3db/goveralls
	goveralls -coverprofile=$(coverfile) -service=semaphore || (echo -e "Coveralls failed" && exit 1)

test-base-ci-integration:
	$(test_ci_integration)
