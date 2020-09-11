# Copyright 2020 Google LLC.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import find_packages
from distutils.core import setup
import os
import sys
import subprocess
from distutils.spawn import find_executable
from distutils.command.clean import clean as _clean
from distutils.command.build_py import build_py as _build_py

if "PROTOC" in os.environ and os.path.exists(os.environ["PROTOC"]):
    protoc = os.environ["PROTOC"]
else:
    protoc = find_executable("protoc")


def generate_proto(source):
    """Invokes the Protocol Compiler to generate a _pb2.py from the given
  .proto file.  Does nothing if the output already exists and is newer than
  the input."""

    output = source.replace(".proto", "_pb2.py")

    if not os.path.exists(output) or (
        os.path.exists(source) and os.path.getmtime(source) > os.path.getmtime(output)
    ):
        print("Generating %s..." % output)

        if not os.path.exists(source):
            sys.stderr.write("Can't find required file: %s\n" % source)
            sys.exit(-1)

        if protoc == None:
            sys.stderr.write(
                "Protocol buffers compiler 'protoc' not installed or not found.\n"
            )
            sys.exit(-1)

        protoc_command = [protoc, "-I.", "--python_out=.", source]
        if subprocess.call(protoc_command) != 0:
            sys.exit(-1)


# List of all .proto files
proto_src = ["simulator/config.proto"]

class build_py(_build_py):
    def run(self):
        for f in proto_src:
            break
            generate_proto(f)
        _build_py.run(self)

class clean(_clean):
    def run(self):
        # Delete generated files in the code tree.
        for (dirpath, dirnames, filenames) in os.walk("."):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                if filepath.endswith("_pb2.py"):
                    os.remove(filepath)
        # _clean is an old-style class, so super() doesn't work.
        _clean.run(self)

REQUIRED_PACKAGES = [
    'numpy',
    'setuptools',
    'protobuf>=3.12.2'
]

setup(
    name="simulator",
    version="0.0.1",
    description="Fortune teller workflow package.",
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    cmdclass={"build_py": build_py, "clean": clean},
)