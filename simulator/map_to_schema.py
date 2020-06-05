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

def MapVMSampleToSchema(vm_sample):
    if vm_sample is not None:
        vm_info = {
            "unique_id": str(vm_sample.get("collection_id"))
            + "-"
            + str(vm_sample.get("instance_index")),
            "collection_id": vm_sample.get("collection_id"),
            "instance_index": vm_sample.get("instance_index"),
            "priority": vm_sample.get("priority", -1),
            "scheduling_class": vm_sample.get("scheduling_class", -1),
            "machine_id": vm_sample.get("machine_id"),
            "alloc_collection_id": vm_sample.get("alloc_collection_id", -1),
            "alloc_instance_index": vm_sample.get("alloc_instance_index", -100),
            "collection_type": vm_sample.get("collection_type", -1),
        }

        vm_metrics = {
            "avg_cpu_usage": vm_sample.get("average_usage", {}).get("cpus", -1),
            "avg_memory_usage": vm_sample.get("average_usage", {}).get("memory", -1),
            "max_cpu_usage": vm_sample.get("maximum_usage", {}).get("cpus", -1),
            "max_memory_usage": vm_sample.get("maximum_usage", {}).get("memory", -1),
            "random_sample_cpu_usage": vm_sample.get("random_sample_usage", {}).get(
                "cpus", -1
            ),
            "assigned_memory": vm_sample.get("assigned_memory", -1),
            "sample_rate": vm_sample.get("sample_rate", -1),
            "p0_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(0, -1),
            "p10_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(1, -1),
            "p20_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(2, -1),
            "p30_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(3, -1),
            "p40_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(4, -1),
            "p50_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(5, -1),
            "p60_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(6, -1),
            "p70_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(7, -1),
            "p80_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(8, -1),
            "p90_cpu_usage": dict(
                enumerate(vm_sample.get("cpu_usage_distribution", []))
            ).get(9, -1),
            "p91_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(0, -1),
            "p92_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(1, -1),
            "p93_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(2, -1),
            "p94_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(3, -1),
            "p95_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(4, -1),
            "p96_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(5, -1),
            "p97_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(6, -1),
            "p98_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(7, -1),
            "p99_cpu_usage": dict(
                enumerate(vm_sample.get("tail_cpu_usage_distribution", []))
            ).get(8, -1),
            "memory_limit": vm_sample.get("resource_request", {}).get("memory", -1),
            "cpu_limit": vm_sample.get("resource_request", {}).get("cpus", -1),
        }

        abstract_metrics = {"usage": 0, "limit": 0}

        updated_vm_sample = {
            "time": vm_sample.get("start_time"),
            "info": vm_info,
            "metrics": vm_metrics,
            "abstract_metrics": abstract_metrics,
        }

    else:
        updated_vm_sample = vm_sample

    return updated_vm_sample
