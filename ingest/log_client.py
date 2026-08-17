"""Kafka-lite: một commit log ghi trên đĩa, API giống kafka-python.

Vì sao không dựng Kafka/Redpanda thật? Nhiệm vụ 5 chỉ cần MỘT tính chất của
Kafka: *offset được commit tách rời khỏi việc ghi dữ liệu*. Tính chất đó tái
hiện đầy đủ bằng một file log + một file offset, mà không bắt cả lớp tải
300 MB image Docker. Ba khái niệm bên dưới map 1-1 sang Kafka thật:

    topic    -> file JSONL, mỗi dòng là một message
    offset   -> số dòng đã được ghi nhận là "đã xử lý"
    commit() -> ghi offset đó xuống đĩa

Đổi sang `kafka-python` chỉ là thay lớp này, không đụng tới consumer.py.

Bạn không cần sửa file này.
"""

from __future__ import annotations

import json
import pathlib


class LogConsumer:
    def __init__(
        self,
        topic: str | pathlib.Path,
        offset_file: str | pathlib.Path,
        group: str = "bronze-loader",
    ) -> None:
        self.topic = pathlib.Path(topic)
        self.offset_file = pathlib.Path(offset_file)
        self.group = group
        self._committed = self._read_offset()
        self._position = self._committed
        self._fh = self.topic.open("r", encoding="utf-8")
        for _ in range(self._position):  # tua tới offset đã commit
            self._fh.readline()

    # ------------------------------------------------------------ offsets
    def _read_offset(self) -> int:
        if not self.offset_file.exists():
            return 0
        try:
            return json.loads(self.offset_file.read_text())[self.group]
        except (ValueError, KeyError):
            return 0

    @property
    def committed_offset(self) -> int:
        return self._committed

    @property
    def position(self) -> int:
        return self._position

    def commit(self) -> None:
        """Ghi nhận: 'mọi message tới vị trí hiện tại coi như đã xử lý xong'."""
        self._committed = self._position
        state = {}
        if self.offset_file.exists():
            try:
                state = json.loads(self.offset_file.read_text())
            except ValueError:
                state = {}
        state[self.group] = self._committed
        self.offset_file.write_text(json.dumps(state), encoding="utf-8")

    # --------------------------------------------------------------- poll
    def poll(self, max_records: int = 500) -> list[dict]:
        batch: list[dict] = []
        while len(batch) < max_records:
            line = self._fh.readline()
            if not line:
                break
            line = line.strip()
            if line:
                batch.append(json.loads(line))
        self._position += len(batch)
        return batch

    def close(self) -> None:
        self._fh.close()

    def __enter__(self) -> "LogConsumer":
        return self

    def __exit__(self, *exc) -> None:
        self.close()
