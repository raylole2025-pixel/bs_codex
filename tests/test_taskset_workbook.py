from __future__ import annotations

import unittest

from apps.run_stage1_workbook_batch import workbook_task_to_payload


class TasksetWorkbookTests(unittest.TestCase):
    def test_workbook_task_to_payload_preserves_emergency_task_type(self) -> None:
        payload = workbook_task_to_payload(
            {
                "task_id": "E1",
                "src_sat": "A1",
                "dst_sat": "B1",
                "arrival_sec": "10",
                "deadline_sec": "20",
                "data_volume_Mb": "30",
                "priority_weight": "4",
                "b_max_Mbps": "5",
                "avg_required_Mbps": "3",
                "task_type": "emg",
                "notes": "role=emergency",
            }
        )

        self.assertEqual(payload["type"], "emg")
        self.assertEqual(payload["preemption_priority"], 4.0)


if __name__ == "__main__":
    unittest.main()
