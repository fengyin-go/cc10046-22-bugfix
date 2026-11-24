"""5条跨模块业务链路测试。"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from fastapi import HTTPException

from app.routers.chains import (
    confirm_crawl_request,
    get_registered_schedule,
    get_source_credential,
    run_crawl_pipeline,
    run_data_process,
    run_reliability,
    run_auth,
    run_delivery,
    validate_crawl_request,
    verify_stored_batch,
)
from app.services import service_for
from app.services.crawler_config import CrawlerConfigService
from app.store import store


def _new_config(name: str = "采集来源", url: str = "https://example.com") -> dict:
    service = service_for("crawler_config")
    entry, _ = service.create_entry(
        {"配置标识": name, "起始地址": url, "页面编码": f"CRAW-{name}"}
    )
    return entry


class TestCrawlPipeline:
    def test_basic(self):
        result = run_crawl_pipeline({
            "config_id": 1,
            "config_name": "test-cfg",
            "period": "daily",
            "batch_label": "B-CP-1",
            "row_count": 100,
            "valid_count": 95,
        })
        assert result["chain"] == "crawl-pipeline"
        steps = result["steps"]
        assert result["accepted"] is True
        assert steps["validate_config"]["config_id"] == 1
        assert steps["create_schedule"]["period"] == "daily"
        assert steps["create_schedule"]["accepted"] is True
        assert steps["track_batch"]["status"] == "执行中"
        assert steps["store_result"]["原始行数"] == "100"

    def test_missing_config_returns_404_and_leaves_no_batch(self):
        schedule_before = len(store.rows("crawl_schedule"))
        batch_before = len(store.rows("batch_tracker"))
        store_before = len(store.rows("result_store"))

        with pytest.raises(HTTPException) as excinfo:
            run_crawl_pipeline({"config_id": 987654, "batch_label": "PHANTOM-1"})
        assert excinfo.value.status_code == 404

        assert len(store.rows("crawl_schedule")) == schedule_before
        assert len(store.rows("batch_tracker")) == batch_before
        assert len(store.rows("result_store")) == store_before
        assert verify_stored_batch(batch_id="PHANTOM-1")["found"] == 0

    def test_dispatch_registers_source_with_real_credential(self):
        entry = _new_config("凭据来源")
        result = run_crawl_pipeline(
            {"config_id": entry["id"], "batch_label": f"CRED-{entry['id']}"}
        )
        credential = result["credential"]
        assert credential is not None
        assert credential["credential_type"] == "采集来源校验凭据"
        assert credential["config_id"] == entry["id"]

        task_id = result["steps"]["create_schedule"]["task_id"]
        schedule = get_registered_schedule(task_id)
        assert schedule["来源配置编号"] == entry["id"]
        assert schedule["凭证标识"] == credential["credential_id"]
        assert schedule["受理状态"] == "已受理"

        looked_up = get_source_credential(credential["credential_id"])
        assert looked_up["config_id"] == entry["id"]
        assert looked_up["config_version"] == credential["config_version"]

    def test_result_store_verifies_config_id_and_version(self):
        entry = _new_config("核对来源")
        batch_id = f"VERIFY-{entry['id']}"
        result = run_crawl_pipeline({"config_id": entry["id"], "batch_label": batch_id})
        version = result["receipt"]["stored_version"]

        matched = verify_stored_batch(
            config_id=entry["id"], config_version=version, batch_id=batch_id
        )
        assert matched["found"] == 1
        assert matched["fake_batch"] is False

        wrong_version = verify_stored_batch(
            config_id=entry["id"], config_version="V0-deadbeef", batch_id=batch_id
        )
        assert wrong_version["found"] == 0
        assert wrong_version["fake_batch"] is True

    def test_same_name_configs_are_distinguished_by_config_id(self):
        first = _new_config("同名来源", "https://one.example.com")
        second = _new_config("同名来源", "https://two.example.com")
        assert first["配置标识"] == second["配置标识"]

        first_result = run_crawl_pipeline(
            {"config_id": first["id"], "batch_label": f"SAME-{first['id']}"}
        )
        second_result = run_crawl_pipeline(
            {"config_id": second["id"], "batch_label": f"SAME-{second['id']}"}
        )

        assert first_result["receipt"]["config_id"] == first["id"]
        assert second_result["receipt"]["config_id"] == second["id"]
        assert verify_stored_batch(
            config_id=first["id"], batch_id=f"SAME-{first['id']}"
        )["found"] == 1
        assert verify_stored_batch(
            config_id=second["id"], batch_id=f"SAME-{first['id']}"
        )["found"] == 0

    def test_disabled_config_is_rejected_without_stored_batch(self):
        entry = _new_config("停用来源")
        run_crawl_pipeline({"config_id": entry["id"], "batch_label": f"PRE-{entry['id']}"})
        service_for("crawler_config").run_action(entry["id"], "停用配置")
        schedule_before = len(store.rows("crawl_schedule"))

        result = run_crawl_pipeline(
            {"config_id": entry["id"], "batch_label": f"POST-{entry['id']}"}
        )
        # 配置存在但已停用：申请被拒（已退回），不是 404，也不生成任何记录。
        assert result["accepted"] is False
        assert result["receipt"]["decision"] == "已退回"
        assert result["receipt"]["task_id"] is None
        assert result["receipt"]["stored"] is False
        assert len(store.rows("crawl_schedule")) == schedule_before
        assert verify_stored_batch(batch_id=f"POST-{entry['id']}")["found"] == 0

    def test_confirmation_rejected_when_config_stopped_between_steps(self):
        entry = _new_config("并发停用来源")
        batch_id = f"RACE-{entry['id']}"

        # 第一阶段：调度拿到真实校验结果与凭据。
        staged = validate_crawl_request({"config_id": entry["id"], "batch_label": batch_id})
        assert staged["request_status"] == "校验完成"
        assert staged["credential"] is not None
        schedule_before = len(store.rows("crawl_schedule"))

        # 确认前值班员停用了配置。
        service_for("crawler_config").run_action(entry["id"], "停用配置")

        result = confirm_crawl_request({"request_id": staged["request_id"]})
        assert result["accepted"] is False
        assert result["receipt"]["decision"] == "已退回"
        assert result["receipt"]["stored"] is False
        assert result["receipt"]["stored_batches"] == []
        assert len(store.rows("crawl_schedule")) == schedule_before
        assert verify_stored_batch(batch_id=batch_id)["found"] == 0

    def test_stale_credential_version_is_rejected(self):
        entry = _new_config("版本漂移来源")
        credential = CrawlerConfigService().domain_validate_config(entry["id"])["credential"]

        # 配置在校验之后被停用，凭据仍是旧版本快照。
        service_for("crawler_config").run_action(entry["id"], "停用配置")
        result = run_crawl_pipeline(
            {
                "config_id": entry["id"],
                "batch_label": f"STALE-{entry['id']}",
                "_credential": credential,
            }
        )
        assert result["accepted"] is False
        assert result["receipt"]["decision"] == "已退回"
        assert result["receipt"]["stored"] is False

    def test_resubmit_after_disable_then_reenable_uses_new_version(self):
        entry = _new_config("重启来源")
        first = run_crawl_pipeline(
            {"config_id": entry["id"], "batch_label": f"BEFORE-{entry['id']}"}
        )
        old_version = first["receipt"]["stored_version"]

        # 停用后重新提交：申请被拒绝，不产生记录。
        service_for("crawler_config").run_action(entry["id"], "停用配置")
        rejected = run_crawl_pipeline(
            {"config_id": entry["id"], "batch_label": f"AFTER-{entry['id']}"}
        )
        assert rejected["accepted"] is False
        assert verify_stored_batch(batch_id=f"AFTER-{entry['id']}")["found"] == 0

        # 重新启用后再提交：版本已变化，落库记录必须是新版本。
        service_for("crawler_config").run_action(entry["id"], "启用配置")
        again = run_crawl_pipeline(
            {"config_id": entry["id"], "batch_label": f"AGAIN-{entry['id']}"}
        )
        assert again["accepted"] is True
        new_version = again["receipt"]["stored_version"]
        assert new_version != old_version
        stored = verify_stored_batch(
            config_id=entry["id"], config_version=new_version,
            batch_id=f"AGAIN-{entry['id']}",
        )
        assert stored["found"] == 1
        assert stored["matches"][0]["采用版本"] == new_version

    def test_result_store_list_entry_point_has_no_phantom_batch(self):
        with pytest.raises(HTTPException) as excinfo:
            run_crawl_pipeline({"config_id": 777001, "batch_label": "NO-SUCH-BATCH"})
        assert excinfo.value.status_code == 404

        # 从结果存储的另一个入口（通用列表接口）核对没有虚假批次。
        items, total = service_for("result_store").list_entries(
            filters={"批次标识": "NO-SUCH-BATCH"})
        assert total == 0 and items == []
        assert verify_stored_batch(batch_id="NO-SUCH-BATCH")["missing"] is True


class TestDataProcess:
    def test_basic(self):
        html = "<html><body><h1>Hello</h1><p>World</p></body></html>"
        result = run_data_process({
            "html": html,
            "source_data": {"name": "Alice", "count": "42"},
            "mapping_rules": [
                {"source": "name", "target": "title", "default": "", "transform": ""},
                {"source": "count", "target": "total", "default": "0", "transform": "int"},
            ],
            "key_fields": ["title"],
        })
        assert result["chain"] == "data-process"
        steps = result["steps"]
        assert steps["parse_html"]["field_count"] >= 1
        assert steps["map_fields"]["title"] == "Alice"
        assert steps["clean_html"]["cleaned_length"] > 0
        assert "dedup" in steps

class TestReliability:
    def test_basic(self):
        result = run_reliability({
            "site": "example.com",
            "protocol": "http",
            "request_count": 100,
            "error_type": "timeout",
            "attempt": 2,
        })
        assert result["chain"] == "reliability"
        steps = result["steps"]
        assert "allocate_proxy" in steps
        assert steps["check_rate"]["site"] == "example.com"
        assert steps["retry_calc"]["error_type"] == "timeout"
        assert steps["log_error"]["error_code"] == "ERR-CHECK"

class TestAuth:
    def test_basic(self):
        result = run_auth({
            "site": "test-auth.com",
            "auth_type": "token",
            "session_type": "http",
        })
        assert result["chain"] == "auth"
        steps = result["steps"]
        assert steps["get_cookies"]["site"] == "test-auth.com"
        assert steps["authenticate"]["authenticated"] is True
        assert steps["create_session"]["status"] == "活跃"

class TestDelivery:
    def test_basic(self):
        result = run_delivery({
            "batch_id": "B-DEL-1",
            "config_name": "delivery-cfg",
            "export_format": "csv",
            "fields": ["id", "title", "count"],
            "cleanup_module": "result_store",
        })
        assert result["chain"] == "delivery"
        steps = result["steps"]
        assert steps["store_result"]["批次标识"] == "B-DEL-1"
        assert steps["export"]["导出格式"] == "csv"
        assert steps["audit"]["操作类型"] == "export"
        assert steps["cleanup"]["任务标识"] == "CLEANUP-B-DEL-1"