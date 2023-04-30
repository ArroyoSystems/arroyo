default: check

check:
    cargo check --all --all-features

docker-single-amd64:
    docker build --build-arg MOLD_ARCH=x86_64 \
        --build-arg PROTO_ARCH=x86_64 \
        --build-arg PROM_ARCH=amd64 \
        --file docker/single/Dockerfile . -t ghcr.io/arroyosystems/arroyo-single:amd64

docker-single-arm64:
    docker build \
        --build-arg MOLD_ARCH=aarch64 \
        --build-arg PROTO_ARCH=aarch_64 \
        --build-arg PROM_ARCH=arm64 \
        --file docker/single/Dockerfile . -t ghcr.io/arroyosystems/arroyo-single:arm64

docker-services-amd64:
    docker build \
        --build-arg PROTO_ARCH=x86_64 \
        --file docker/cluster/services/Dockerfile . -t ghcr.io/arroyosystems/arroyo-services:amd64

docker-services-arm64:
    docker build \
        --build-arg PROTO_ARCH=aarch_64 \
        --file docker/cluster/services/Dockerfile . -t ghcr.io/arroyosystems/arroyo-services:arm64

docker-compiler-amd64:
    docker build \
        --build-arg MOLD_ARCH=x86_64 \
        --build-arg PROTO_ARCH=x86_64 \
        --file docker/cluster/compiler/Dockerfile . -t ghcr.io/arroyosystems/arroyo-compiler:amd64

docker-compiler-arm64:
    docker build \
        --build-arg MOLD_ARCH=aarch64 \
        --build-arg PROTO_ARCH=aarch_64 \
        --file docker/cluster/compiler/Dockerfile . -t ghcr.io/arroyosystems/arroyo-compiler:arm64

docker-amd64: docker-single-amd64 docker-services-amd64 docker-compiler-amd64

docker-arm64: docker-single-arm64 docker-services-arm64 docker-compiler-arm64

docker-cluster-amd64: docker-services-amd64 docker-compiler-amd64
docker-cluster-arm64: docker-services-arm64 docker-compiler-arm64
