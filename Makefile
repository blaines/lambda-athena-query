gopath = $(shell git config --get remote.origin.url | cut -d/ -f3-)
git_sha = $(shell git rev-parse --short HEAD)
git_branch = $(shell git rev-parse --abbrev-ref HEAD)
git_summary = $(shell git describe --tags --dirty --always)
git_tags = $(shell git describe --tags --always)
build_date = $(shell date)
version = $(shell cat VERSION)

deps:
	glide install -v

build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
	-a -installsuffix cgo \
	-ldflags "-X 'main.Version=${version}' -X 'main.GitSummary=${git_summary}' -X 'main.BuildDate=${build_date}' -X main.GitCommit=${git_sha} -X main.GitBranch=${git_branch}" \
	-o build/aq-linux-amd64 cli/*.go
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build \
	-a -installsuffix cgo \
	-ldflags "-X 'main.Version=${version}' -X 'main.GitSummary=${git_summary}' -X 'main.BuildDate=${build_date}' -X main.GitCommit=${git_sha} -X main.GitBranch=${git_branch}" \
	-o build/aq-darwin-amd64 cli/*.go

build-docker:
	docker build -t blaines/aq:${git_sha} .
	docker tag blaines/aq:${git_sha} blaines/aq:${version}-${git_tags}

upload:
	docker push blaines/aq:${git_sha}
	docker push blaines/aq:${version}-${git_tags}

.PHONY: build
