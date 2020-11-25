# -*- coding: utf-8 -*-
"""Monitoring model."""
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, JSON, String

from pipelines.database import Base
from pipelines.utils import to_camel_case


class Monitoring(Base):
    __tablename__ = "monitoring"
    uuid = Column(String(255), primary_key=True)
    project_id = Column(String(255), ForeignKey("projects.uuid"), nullable=False)
    experiment_id = Column(String(255), ForeignKey("experiments.uuid"))
    operator_id = Column(String(255))
    run_id = Column(String(255))
    layout = Column(JSON)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    def __repr__(self):
        return f"<Monitoring {self.uuid}>"

    def as_dict(self):
        d = {to_camel_case(c.name): getattr(self, c.name) for c in self.__table__.columns}
        return d
