"""应用内使用的中文业务异常。"""

from __future__ import annotations


class LabError(Exception):
    """所有业务异常的基类。"""


class InputError(LabError):
    """上传内容不合法，整批拒绝。"""

    def __init__(self, messages):
        if isinstance(messages, str):
            messages = [messages]
        self.messages = list(messages)
        super().__init__("；".join(self.messages))


class ConflictError(LabError):
    """修订号冲突。"""

    def __init__(self, message, current_revision=None):
        self.current_revision = current_revision
        super().__init__(message)


class PublishError(LabError):
    """文件写入或发布登记失败。"""


class NotFoundError(LabError):
    """批次 / 修订不存在。"""
