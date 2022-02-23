The ghOSt userspace component can be compiled on Ubuntu 20.04 or newer.

1\. We use the Google Bazel build system to compile the userspace components of
ghOSt. Go to the
[Bazel Installation Guide](https://docs.bazel.build/versions/main/install.html)
for instructions to install Bazel on your operating system.

2\. Install ghOSt dependencies:

```
# yum install -y make gcc rsync
cd ~/ghost-kernel
make headers_install INSTALL_HDR_PATH=/usr
apt update
apt install -y libnuma-dev libcap-dev libelf-dev libbfd-dev clang llvm zlib1g-dev python-is-python3 pip
# or yum install -y numactl-libs libcap elfutils-libelf clang llvm python3
```

3\. Compile the ghOSt userspace component. Run the following from the root of
the repository:

apt install -y curl gnupg
curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg
mv bazel.gpg /etc/apt/trusted.gpg.d/
echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list
apt update && apt install -y bazel

cd ~/ghost-userspace/
bazel build -c opt ...

`-c opt` tells Bazel to build the targets with optimizations turned on. `...`
tells Bazel to build all targets in the `BUILD` file and all `BUILD` files in
subdirectories, including the core ghOSt library, the eBPF code, the schedulers,
the unit tests, the experiments, and the scripts to run the experiments, along
with all of the dependencies for those targets. If you prefer to build
individual targets rather than all of them to save compile time, replace `...`
with an individual target name, such as `agent_shinjuku`.
