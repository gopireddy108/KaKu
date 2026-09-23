import sys
import xml.etree.ElementTree as ET
from collections import defaultdict, deque
from typing import Dict, List, Set, Tuple


class ControlMValidator:
    def __init__(self, xml_filepath: str):
        self.xml_filepath = xml_filepath
        self.tree = None
        self.root = None
        self.jobs: Dict[str, ET.Element] = {}
        self.in_conditions: Dict[str, List[Tuple[str, str]]] = defaultdict(list)  # job_name -> [(cond_name, date)]
        self.out_conditions: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list) # job_name -> [(cond_name, date, sign)]
        self.errors: List[str] = []
        self.warnings: List[str] = []

    def load_xml(self) -> bool:
        """Parse the Control-M XML export file."""
        try:
            self.tree = ET.parse(self.xml_filepath)
            self.root = self.tree.getroot()
            return True
        except ET.ParseError as e:
            self.errors.append(f"Fatal: Invalid XML structure - {e}")
            return False
        except Exception as e:
            self.errors.append(f"Fatal: Could not read file - {e}")
            return False

    def extract_definitions(self):
        """Index all JOB elements and harvest IN/OUT conditions."""
        # Find all JOB nodes (handles both root elements and nested SMART FOLDER structures)
        for job in self.root.iter('JOB'):
            job_name = job.attrib.get('JOBNAME')
            if not job_name:
                self.errors.append("Validation Error: Found a JOB element missing the 'JOBNAME' attribute.")
                continue

            if job_name in self.jobs:
                self.warnings.append(f"Duplicate Job Name: '{job_name}' is defined multiple times in this template.")
            
            self.jobs[job_name] = job

            # Extract In-Conditions
            for in_cond in job.findall('INCOND'):
                cond_name = in_cond.attrib.get('NAME')
                odate = in_cond.attrib.get('ODAT', 'ODAT')
                if cond_name:
                    self.in_conditions[job_name].append((cond_name, odate))
                else:
                    self.errors.append(f"Job '{job_name}' has an INCOND missing a NAME attribute.")

            # Extract Out-Conditions
            for out_cond in job.findall('OUTCOND'):
                cond_name = out_cond.attrib.get('NAME')
                odate = out_cond.attrib.get('ODAT', 'ODAT')
                sign = out_cond.attrib.get('SIGN', '+')  # '+' adds, '-' deletes/cleans up
                if cond_name:
                    self.out_conditions[job_name].append((cond_name, odate, sign))
                else:
                    self.errors.append(f"Job '{job_name}' has an OUTCOND missing a NAME attribute.")

    def validate_missing_predecessors(self):
        """Identify INCOND prerequisites that are never produced by any job in the XML."""
        # Map created condition names to the producing job
        created_conditions: Dict[str, Set[str]] = defaultdict(set)
        for job_name, conds in self.out_conditions.items():
            for cond_name, odate, sign in conds:
                if sign == '+':
                    created_conditions[cond_name].add(job_name)

        # Check every job's required INCOND
        for job_name, conds in self.in_conditions.items():
            for cond_name, odate in conds:
                if cond_name not in created_conditions:
                    self.errors.append(
                        f"Missing Predecessor: Job '{job_name}' expects INCOND '{cond_name}', "
                        f"but no job in this export generates it as an OUTCOND."
                    )

    def validate_dangling_outputs(self):
        """Warn if a job produces an OUTCOND that no other job consumes (orphaned output)."""
        required_conditions = {cond_name for cond_list in self.in_conditions.values() for cond_name, _ in cond_list}

        for job_name, conds in self.out_conditions.items():
            for cond_name, odate, sign in conds:
                if sign == '+' and cond_name not in required_conditions:
                    self.warnings.append(
                        f"Unused Output Condition: Job '{job_name}' posts OUTCOND '{cond_name}', "
                        f"but no downstream job in this export requires it."
                    )

    def validate_cycles(self):
        """Detect circular dependencies (deadlocks) using Kahn's Algorithm (Topological Sort)."""
        # Step 1: Map conditions to producing jobs
        condition_producers: Dict[str, List[str]] = defaultdict(list)
        for job_name, conds in self.out_conditions.items():
            for cond_name, _, sign in conds:
                if sign == '+':
                    condition_producers[cond_name].append(job_name)

        # Step 2: Build adjacency graph (parent_job -> child_jobs)
        adjacency: Dict[str, Set[str]] = defaultdict(set)
        in_degree: Dict[str, int] = {job: 0 for job in self.jobs}

        for child_job, conds in self.in_conditions.items():
            for cond_name, _ in conds:
                parents = condition_producers.get(cond_name, [])
                for parent_job in parents:
                    if parent_job != child_job:  # Exclude self-loop check handled separately
                        if child_job not in adjacency[parent_job]:
                            adjacency[parent_job].add(child_job)
                            in_degree[child_job] += 1
                    else:
                        self.errors.append(f"Self Loop: Job '{job_name}' depends on its own OUTCOND '{cond_name}'.")

        # Step 3: Kahn's Algorithm
        queue = deque([job for job, degree in in_degree.items() if degree == 0])
        processed_count = 0

        while queue:
            node = queue.popleft()
            processed_count += 1
            for neighbor in adjacency[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        # If processed nodes != total nodes, a cycle exists
        if processed_count < len(self.jobs):
            cyclic_jobs = [job for job, degree in in_degree.items() if degree > 0]
            self.errors.append(
                f"Cyclic Dependency Detected (Deadlock): The following jobs form a closed loop: {cyclic_jobs}"
            )

    def validate_node_groups_and_variables(self):
        """Ensure critical attributes (Host/NodeID, Application, Sub-Application) are present."""
        for job_name, job_elem in self.jobs.items():
            node_id = job_elem.attrib.get('NODEID') or job_elem.attrib.get('HOST')
            if not node_id:
                self.warnings.append(f"Configuration Warning: Job '{job_name}' has no NODEID or HOST specified.")

            app = job_elem.attrib.get('APPLICATION')
            sub_app = job_elem.attrib.get('SUB_APPLICATION')
            if not app or not sub_app:
                self.warnings.append(f"Metadata Warning: Job '{job_name}' is missing APPLICATION or SUB_APPLICATION tags.")

    def run_all_checks() -> bool:
        """Executes all validation routines."""
        if not self.load_xml():
            return False

        self.extract_definitions()
        if not self.jobs:
            self.errors.append("Validation Failed: No <JOB> definitions found in the provided XML.")
            return False

        self.validate_missing_predecessors()
        self.validate_dangling_outputs()
        self.validate_cycles()
        self.validate_node_groups_and_variables()

        return len(self.errors) == 0


def main():
    if len(sys.argv) < 2:
        print("Usage: python controlm_validator.py <path_to_controlm_export.xml>")
        sys.exit(1)

    xml_path = sys.argv[1]
    print(f"--- Control-M Pre-Deployment Validation: {xml_path} ---\n")

    validator = ControlMValidator(xml_path)
    passed = validator.run_all_checks()

    if validator.warnings:
        print(" [WARNINGS]")
        for warn in validator.warnings:
            print(f"  - {warn}")
        print()

    if validator.errors:
        print(" [ERRORS]")
        for err in validator.errors:
            print(f"  - {err}")
        print()
        print("Result: DEPLOYMENT BLOCKED - Critical validation failures found.")
        sys.exit(1)
    else:
        print("Result: PASSED - All dependency and structural checks succeeded.")
        sys.exit(0)


if __name__ == "__main__":
    main()

