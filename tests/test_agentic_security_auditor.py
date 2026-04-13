"""
Tests for 43_agentic_security_auditor.py

Covers AgenticSystemConfig, AuditFinding, AgentAuditReport, and
AgenticSecurityAuditor across all seven security domains.

~40 test cases using pytest and importlib loading pattern.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import types

# ---------------------------------------------------------------------------
# Load the example module via importlib
# ---------------------------------------------------------------------------

_MOD_NAME = "agentic_security_auditor_43"
_EXAMPLE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "examples", "43_agentic_security_auditor.py"
)

spec = importlib.util.spec_from_file_location(_MOD_NAME, _EXAMPLE_PATH)
mod = types.ModuleType(_MOD_NAME)
sys.modules[_MOD_NAME] = mod
spec.loader.exec_module(mod)  # type: ignore[union-attr]

AgenticSystemConfig = mod.AgenticSystemConfig
AuditFinding = mod.AuditFinding
AgentAuditReport = mod.AgentAuditReport
AgenticSecurityAuditor = mod.AgenticSecurityAuditor


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _all_off_config(**overrides) -> AgenticSystemConfig:
    """Return a fully-default (all-disabled) AgenticSystemConfig with optional overrides."""
    return AgenticSystemConfig(**overrides)


def _all_on_config() -> AgenticSystemConfig:
    """Return an AgenticSystemConfig with every control enabled."""
    return AgenticSystemConfig(
        system_id="fully-hardened",
        tool_permission_model="rbac",
        tool_scope_enforced=True,
        tool_output_validated=True,
        unsafe_tool_calls_blocked=True,
        mcp_enabled=True,
        mcp_source_allowlisted=True,
        mcp_checksum_verified=True,
        mcp_rate_limiting_enabled=True,
        mcp_invocation_audit_logged=True,
        agent_identity_enforced=True,
        caller_context_propagated=True,
        privilege_escalation_prevented=True,
        memory_access_scoped=True,
        prompt_context_sanitized=True,
        long_term_memory_encrypted=True,
        multi_agent_enabled=True,
        agent_trust_boundaries_defined=True,
        inter_agent_message_signed=True,
        agent_composition_validated=True,
        tool_invocation_logging=True,
        reasoning_trace_captured=True,
        hitl_for_high_risk_actions=True,
        autonomous_action_rate_limited=True,
        incident_response_playbook=True,
    )


def _run(config: AgenticSystemConfig) -> AgentAuditReport:
    return AgenticSecurityAuditor(config).run_audit()


def _find(report: AgentAuditReport, control_id: str) -> AuditFinding:
    for f in report.findings:
        if f.control_id == control_id:
            return f
    raise AssertionError(f"Control {control_id!r} not found in findings")


# ---------------------------------------------------------------------------
# AgenticSystemConfig defaults
# ---------------------------------------------------------------------------


class TestAgenticSystemConfigDefaults:
    def test_system_id_default(self):
        assert AgenticSystemConfig().system_id == "agentic-system"

    def test_tool_permission_model_default_none(self):
        assert AgenticSystemConfig().tool_permission_model == "none"

    def test_mcp_enabled_default_false(self):
        assert AgenticSystemConfig().mcp_enabled is False

    def test_multi_agent_enabled_default_false(self):
        assert AgenticSystemConfig().multi_agent_enabled is False

    def test_all_bool_fields_default_false(self):
        cfg = AgenticSystemConfig()
        bool_fields = [
            "tool_scope_enforced", "tool_output_validated", "unsafe_tool_calls_blocked",
            "mcp_enabled", "mcp_source_allowlisted", "mcp_checksum_verified",
            "mcp_rate_limiting_enabled", "mcp_invocation_audit_logged",
            "agent_identity_enforced", "caller_context_propagated",
            "privilege_escalation_prevented", "memory_access_scoped",
            "prompt_context_sanitized", "long_term_memory_encrypted",
            "multi_agent_enabled", "agent_trust_boundaries_defined",
            "inter_agent_message_signed", "agent_composition_validated",
            "tool_invocation_logging", "reasoning_trace_captured",
            "hitl_for_high_risk_actions", "autonomous_action_rate_limited",
            "incident_response_playbook",
        ]
        for field in bool_fields:
            assert getattr(cfg, field) is False, f"{field} should default to False"


# ---------------------------------------------------------------------------
# All-defaults audit
# ---------------------------------------------------------------------------


class TestAllDefaultsAudit:
    def setup_method(self):
        self.report = _run(_all_off_config())

    def test_score_is_zero(self):
        assert self.report.score == 0

    def test_maturity_is_sandbox(self):
        assert self.report.maturity_level == "Sandbox"

    def test_critical_failures_gt_zero(self):
        assert len(self.report.critical_failures()) > 0

    def test_findings_is_list(self):
        assert isinstance(self.report.findings, list)

    def test_summary_is_string(self):
        assert isinstance(self.report.summary, str)


# ---------------------------------------------------------------------------
# All-enabled audit
# ---------------------------------------------------------------------------


class TestAllEnabledAudit:
    def setup_method(self):
        self.report = _run(_all_on_config())

    def test_score_is_100(self):
        assert self.report.score == 100

    def test_maturity_is_autonomous(self):
        assert self.report.maturity_level == "Autonomous"

    def test_zero_critical_failures(self):
        assert len(self.report.critical_failures()) == 0

    def test_zero_high_failures(self):
        assert len(self.report.high_failures()) == 0


# ---------------------------------------------------------------------------
# MCP Security domain
# ---------------------------------------------------------------------------


class TestMCPControls:
    def test_mcp_disabled_all_mcp_controls_skip(self):
        report = _run(_all_off_config(mcp_enabled=False))
        mcp_findings = [f for f in report.findings if f.control_id.startswith("AGT-MCP-")]
        assert len(mcp_findings) > 0
        for f in mcp_findings:
            assert f.status == "SKIP", (
                f"{f.control_id} should be SKIP when mcp_enabled=False, got {f.status}"
            )

    def test_mcp_enabled_source_not_allowlisted_is_fail_critical(self):
        report = _run(_all_off_config(mcp_enabled=True, mcp_source_allowlisted=False))
        finding = _find(report, "AGT-MCP-001")
        assert finding.status == "FAIL"
        assert finding.severity == "CRITICAL"

    def test_mcp_all_controls_pass_when_fully_configured(self):
        cfg = _all_off_config(
            mcp_enabled=True,
            mcp_source_allowlisted=True,
            mcp_checksum_verified=True,
            mcp_rate_limiting_enabled=True,
            mcp_invocation_audit_logged=True,
        )
        report = _run(cfg)
        mcp_findings = [f for f in report.findings if f.control_id.startswith("AGT-MCP-")]
        for f in mcp_findings:
            assert f.status == "PASS", (
                f"{f.control_id} should PASS when all MCP flags True, got {f.status}"
            )

    def test_mcp_enabled_no_mcp_controls_are_skip(self):
        """When mcp_enabled=True, MCP controls should NOT be SKIP."""
        report = _run(_all_off_config(mcp_enabled=True))
        mcp_findings = [f for f in report.findings if f.control_id.startswith("AGT-MCP-")]
        skip_count = sum(1 for f in mcp_findings if f.status == "SKIP")
        assert skip_count == 0


# ---------------------------------------------------------------------------
# Multi-Agent domain
# ---------------------------------------------------------------------------


class TestMultiAgentControls:
    def test_multi_agent_disabled_all_ma_controls_skip(self):
        report = _run(_all_off_config(multi_agent_enabled=False))
        ma_findings = [f for f in report.findings if f.control_id.startswith("AGT-MA-")]
        assert len(ma_findings) > 0
        for f in ma_findings:
            assert f.status == "SKIP", (
                f"{f.control_id} should be SKIP when multi_agent_enabled=False, got {f.status}"
            )

    def test_multi_agent_enabled_trust_boundaries_not_defined_is_fail_critical(self):
        report = _run(
            _all_off_config(multi_agent_enabled=True, agent_trust_boundaries_defined=False)
        )
        finding = _find(report, "AGT-MA-001")
        assert finding.status == "FAIL"
        assert finding.severity == "CRITICAL"


# ---------------------------------------------------------------------------
# Tool Permission domain
# ---------------------------------------------------------------------------


class TestToolPermissionControls:
    def test_tool_permission_model_rbac_passes_agt_tp_001(self):
        report = _run(_all_off_config(tool_permission_model="rbac"))
        finding = _find(report, "AGT-TP-001")
        assert finding.status == "PASS"

    def test_tool_permission_model_none_fails_critical_agt_tp_001(self):
        report = _run(_all_off_config(tool_permission_model="none"))
        finding = _find(report, "AGT-TP-001")
        assert finding.status == "FAIL"
        assert finding.severity == "CRITICAL"

    def test_tool_permission_model_allowlist_passes_agt_tp_001(self):
        report = _run(_all_off_config(tool_permission_model="allowlist"))
        finding = _find(report, "AGT-TP-001")
        assert finding.status == "PASS"


# ---------------------------------------------------------------------------
# HITL domain
# ---------------------------------------------------------------------------


class TestHITLControls:
    def test_hitl_for_high_risk_actions_true_passes_agt_hitl_001(self):
        report = _run(_all_off_config(hitl_for_high_risk_actions=True))
        finding = _find(report, "AGT-HITL-001")
        assert finding.status == "PASS"

    def test_hitl_for_high_risk_actions_false_fails_critical(self):
        report = _run(_all_off_config(hitl_for_high_risk_actions=False))
        finding = _find(report, "AGT-HITL-001")
        assert finding.status == "FAIL"
        assert finding.severity == "CRITICAL"


# ---------------------------------------------------------------------------
# Identity & Authorization domain
# ---------------------------------------------------------------------------


class TestIdentityAuthorizationControls:
    def test_privilege_escalation_prevented_false_is_fail_critical(self):
        report = _run(_all_off_config(privilege_escalation_prevented=False))
        finding = _find(report, "AGT-IA-003")
        assert finding.status == "FAIL"
        assert finding.severity == "CRITICAL"

    def test_privilege_escalation_prevented_true_is_pass(self):
        report = _run(_all_off_config(privilege_escalation_prevented=True))
        finding = _find(report, "AGT-IA-003")
        assert finding.status == "PASS"


# ---------------------------------------------------------------------------
# Memory & Context domain
# ---------------------------------------------------------------------------


class TestMemoryContextControls:
    def test_prompt_context_sanitized_true_passes_agt_mc_002(self):
        report = _run(_all_off_config(prompt_context_sanitized=True))
        finding = _find(report, "AGT-MC-002")
        assert finding.status == "PASS"

    def test_prompt_context_sanitized_false_fails(self):
        report = _run(_all_off_config(prompt_context_sanitized=False))
        finding = _find(report, "AGT-MC-002")
        assert finding.status == "FAIL"


# ---------------------------------------------------------------------------
# AgentAuditReport helper methods
# ---------------------------------------------------------------------------


class TestAgentAuditReportHelpers:
    def test_by_status_returns_correct_findings(self):
        report = _run(_all_off_config())
        fail_findings = report.by_status("FAIL")
        assert all(f.status == "FAIL" for f in fail_findings)

    def test_by_severity_returns_correct_findings(self):
        report = _run(_all_off_config())
        critical_findings = report.by_severity("CRITICAL")
        assert all(f.severity == "CRITICAL" for f in critical_findings)

    def test_critical_failures_subset_of_fail(self):
        report = _run(_all_off_config())
        cf = report.critical_failures()
        assert all(f.status == "FAIL" and f.severity == "CRITICAL" for f in cf)

    def test_high_failures_subset_of_fail(self):
        report = _run(_all_off_config())
        hf = report.high_failures()
        assert all(f.status == "FAIL" and f.severity == "HIGH" for f in hf)

    def test_skip_findings_do_not_reduce_score(self):
        """MCP disabled → SKIP findings; score should not be lower than a config
        where MCP is simply absent."""
        report_no_mcp = _run(_all_off_config(mcp_enabled=False))
        report_mcp_on_partial = _run(_all_off_config(mcp_enabled=True))
        # Enabling MCP without proper config adds FAILs → lower score
        assert report_no_mcp.score >= report_mcp_on_partial.score

    def test_framework_refs_nonempty_for_all_findings(self):
        report = _run(_all_off_config())
        for f in report.findings:
            assert len(f.framework_refs) > 0, (
                f"{f.control_id} has empty framework_refs"
            )
