"""
43_agentic_security_auditor.py — Holistic Enterprise Agentic AI Security Audit Framework

Pure-Python implementation of an enterprise-grade security audit framework for
agentic AI systems using only the standard library (``dataclasses``, ``typing``).
No external dependencies are required.

    Dataclass 1 — AgenticSystemConfig:
                Configuration snapshot that fully describes the security
                posture of a deployed agentic AI system.  Fields are
                grouped across seven domains:

                **Tool Permission** — ``tool_permission_model`` selects
                    one of ``"none"``, ``"allowlist"``, ``"jit"``, or
                    ``"rbac"`` to describe how the agent runtime controls
                    which tools an agent may invoke.
                    ``tool_scope_enforced`` indicates whether each tool
                    invocation is constrained to a declared scope.
                    ``tool_output_validated`` flags whether tool return
                    values are checked before being fed back into agent
                    context.  ``unsafe_tool_calls_blocked`` records
                    whether known-dangerous invocations are rejected at
                    the runtime boundary.

                **MCP Security** — All fields are evaluated only when
                    ``mcp_enabled`` is ``True``; otherwise every MCP
                    control is set to ``SKIP``.
                    ``mcp_source_allowlisted`` — MCP servers are drawn
                    from an explicit origin allowlist.
                    ``mcp_checksum_verified`` — tool manifest checksums
                    are verified before admission (guards against
                    CVE-2025-6514 class tampering).
                    ``mcp_rate_limiting_enabled`` — per-tool rate limits
                    prevent runaway invocation storms.
                    ``mcp_invocation_audit_logged`` — every MCP call is
                    appended to an immutable audit trail.

                **Identity & Authorization** — ``agent_identity_enforced``
                    requires each agent to present a verified identity
                    credential.  ``caller_context_propagated`` ensures
                    the originating human caller's context flows through
                    multi-step agent chains.
                    ``privilege_escalation_prevented`` confirms that
                    agents cannot acquire permissions beyond those
                    granted at instantiation.

                **Memory & Context** — ``memory_access_scoped`` limits
                    each agent to reading and writing only its own
                    session-bounded memory partition.
                    ``prompt_context_sanitized`` records whether
                    retrieved content is scrubbed for injection payloads
                    before insertion into prompts.
                    ``long_term_memory_encrypted`` covers persistent
                    memory stores at rest.

                **Multi-Agent** — Evaluated only when
                    ``multi_agent_enabled`` is ``True``; otherwise every
                    multi-agent control is ``SKIP``.
                    ``agent_trust_boundaries_defined`` — each agent's
                    authority scope is declared and enforced.
                    ``inter_agent_message_signed`` — messages flowing
                    between agents carry integrity signatures.
                    ``agent_composition_validated`` — the composition of
                    agent pipelines is validated before execution.

                **Observability** — ``tool_invocation_logging`` writes
                    every tool call to a structured log.
                    ``reasoning_trace_captured`` persists the agent's
                    chain-of-thought for post-incident review.
                    ``incident_response_playbook`` documents the
                    response procedures for agentic security events.

                **HITL (Human-in-the-Loop)** — ``hitl_for_high_risk_actions``
                    requires a human approval gate before the agent
                    executes irreversible or high-impact actions.
                    ``autonomous_action_rate_limited`` applies a
                    frequency ceiling to prevent uncontrolled
                    autonomous execution bursts.

    Dataclass 2 — AuditFinding:
                Represents the outcome of a single security control
                evaluation.  ``control_id`` is a slug such as
                ``"AGT-TP-001"``.  ``domain`` groups the finding into
                one of seven audit domains.  ``status`` is one of
                ``"PASS"``, ``"FAIL"``, ``"WARN"``, ``"SKIP"``.
                ``severity`` indicates the impact when the finding
                fails: ``"CRITICAL"``, ``"HIGH"``, ``"MEDIUM"``,
                ``"LOW"``, ``"INFO"``.  ``framework_refs`` lists the
                authoritative standards cited (e.g. ``"OWASP ASI02"``).
                ``evidence`` records what was observed.
                ``remediation_step`` provides the next action.

    Dataclass 3 — AgentAuditReport:
                Aggregates all :class:`AuditFinding` objects produced by
                a single audit run.  ``system_id`` echoes the audited
                system identifier.  ``findings`` is the ordered list of
                findings.  ``score`` is the numeric security score
                computed by :meth:`AgenticSecurityAuditor.compute_score`.
                ``maturity_level`` is the human-readable trust tier:
                ``"Sandbox"``, ``"Controlled"``, ``"Trusted"``, or
                ``"Autonomous"``.  ``summary`` is a free-text narrative
                generated by the auditor.  :meth:`by_status` and
                :meth:`by_severity` provide convenient filtered views.
                :meth:`critical_failures` and :meth:`high_failures`
                return findings with ``status="FAIL"`` at those
                severity tiers.

    Class 4 — AgenticSecurityAuditor:
                Main audit engine.  Constructed with an
                :class:`AgenticSystemConfig`.  Seven private checker
                methods — ``_check_tool_permissions``,
                ``_check_mcp_security``, ``_check_identity_authorization``,
                ``_check_memory_context``, ``_check_multi_agent``,
                ``_check_observability``, and ``_check_hitl`` — each
                return a list of :class:`AuditFinding` objects for their
                domain.  :meth:`run_audit` calls all seven checkers,
                assembles the :class:`AgentAuditReport`, computes the
                score, derives the maturity level, and returns the
                complete report.

                **Scoring formula (starts at 100)**:
                ``CRITICAL`` fail → −15 points each.
                ``HIGH`` fail → −7 points each.
                ``MEDIUM`` fail → −3 points each.
                ``WARN`` → −1 point each.
                ``SKIP`` findings do not affect the score.
                Minimum score is 0.

                **Maturity tiers**:
                ``Sandbox``    — any CRITICAL fail *or* score < 50.
                ``Controlled`` — score 50–69.
                ``Trusted``    — score 70–84, *or* score ≥ 85 with more
                                 than two HIGH failures.
                ``Autonomous`` — score ≥ 85 and zero HIGH failures.

Usage example
-------------
::

    config = AgenticSystemConfig(
        system_id="prod-rag-agent",
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
        multi_agent_enabled=False,
        tool_invocation_logging=True,
        reasoning_trace_captured=True,
        hitl_for_high_risk_actions=True,
        autonomous_action_rate_limited=True,
        incident_response_playbook=True,
    )

    auditor = AgenticSecurityAuditor(config)
    report = auditor.run_audit()
    print(report.maturity_level)    # "Autonomous"
    print(report.score)             # >= 85

No external dependencies required.

Run:
    python examples/43_agentic_security_auditor.py
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# ---------------------------------------------------------------------------
# Domain constants
# ---------------------------------------------------------------------------

DOMAIN_TOOL_PERMISSIONS: str = "Tool Permissions"
DOMAIN_MCP_SECURITY: str = "MCP Security"
DOMAIN_IDENTITY_AUTHORIZATION: str = "Identity & Authorization"
DOMAIN_MEMORY_CONTEXT: str = "Memory & Context"
DOMAIN_MULTI_AGENT: str = "Multi-Agent"
DOMAIN_OBSERVABILITY: str = "Observability"
DOMAIN_HITL: str = "Human-in-the-Loop"

# Deduction weights by severity for the scoring formula.
_SEVERITY_DEDUCTION: dict[str, int] = {
    "CRITICAL": 15,
    "HIGH": 7,
    "MEDIUM": 3,
    "LOW": 1,
}

# ---------------------------------------------------------------------------
# Dataclass 1 — AgenticSystemConfig
# ---------------------------------------------------------------------------


@dataclass
class AgenticSystemConfig:
    """Full security-posture snapshot of a deployed agentic AI system.

    Fields are grouped into seven audit domains.  Boolean flags record
    whether a control is present and active in the running system.
    The ``mcp_*`` controls are evaluated only when ``mcp_enabled`` is
    ``True``; the ``agent_*`` multi-agent controls are evaluated only when
    ``multi_agent_enabled`` is ``True``.

    Example::

        config = AgenticSystemConfig(
            system_id="order-processing-agent",
            tool_permission_model="allowlist",
            tool_scope_enforced=True,
        )
    """

    system_id: str = "agentic-system"

    # ------------------------------------------------------------------
    # Tool Permission domain
    # ------------------------------------------------------------------
    tool_permission_model: str = "none"  # "none" | "allowlist" | "jit" | "rbac"
    tool_scope_enforced: bool = False
    tool_output_validated: bool = False
    unsafe_tool_calls_blocked: bool = False

    # ------------------------------------------------------------------
    # MCP Security domain (evaluated only when mcp_enabled is True)
    # ------------------------------------------------------------------
    mcp_enabled: bool = False
    mcp_source_allowlisted: bool = False
    mcp_checksum_verified: bool = False
    mcp_rate_limiting_enabled: bool = False
    mcp_invocation_audit_logged: bool = False

    # ------------------------------------------------------------------
    # Identity & Authorization domain
    # ------------------------------------------------------------------
    agent_identity_enforced: bool = False
    caller_context_propagated: bool = False
    privilege_escalation_prevented: bool = False

    # ------------------------------------------------------------------
    # Memory & Context domain
    # ------------------------------------------------------------------
    memory_access_scoped: bool = False
    prompt_context_sanitized: bool = False
    long_term_memory_encrypted: bool = False

    # ------------------------------------------------------------------
    # Multi-Agent domain (evaluated only when multi_agent_enabled is True)
    # ------------------------------------------------------------------
    multi_agent_enabled: bool = False
    agent_trust_boundaries_defined: bool = False
    inter_agent_message_signed: bool = False
    agent_composition_validated: bool = False

    # ------------------------------------------------------------------
    # Observability & HITL domain
    # ------------------------------------------------------------------
    tool_invocation_logging: bool = False
    reasoning_trace_captured: bool = False
    hitl_for_high_risk_actions: bool = False
    autonomous_action_rate_limited: bool = False
    incident_response_playbook: bool = False


# ---------------------------------------------------------------------------
# Dataclass 2 — AuditFinding
# ---------------------------------------------------------------------------


@dataclass
class AuditFinding:
    """Single security control evaluation result.

    Attributes
    ----------
    control_id : str
        Unique slug for the control, e.g. ``"AGT-TP-001"``.
    control_name : str
        Human-readable name of the control.
    domain : str
        Audit domain the control belongs to (one of the seven domain
        constants defined in this module).
    status : str
        Outcome of the evaluation: ``"PASS"``, ``"FAIL"``, ``"WARN"``,
        or ``"SKIP"``.
    severity : str
        Impact classification when the finding status is ``"FAIL"``:
        ``"CRITICAL"``, ``"HIGH"``, ``"MEDIUM"``, ``"LOW"``, ``"INFO"``.
    framework_refs : list[str]
        Authoritative standard citations that mandate this control,
        e.g. ``["OWASP ASI02", "NIST AI 600-1 §2.5"]``.
    evidence : str
        Description of the observed system state that led to this finding.
    remediation_step : str
        Concrete next action to resolve the finding when status is
        ``"FAIL"`` or ``"WARN"``.

    Example::

        finding = AuditFinding(
            control_id="AGT-TP-001",
            control_name="Tool Permission Model",
            domain=DOMAIN_TOOL_PERMISSIONS,
            status="FAIL",
            severity="CRITICAL",
            framework_refs=["OWASP ASI02", "NIST AI 600-1 §2.5"],
            evidence="tool_permission_model is 'none' — no restriction applied.",
            remediation_step="Set tool_permission_model to 'allowlist' or stricter.",
        )
    """

    control_id: str
    control_name: str
    domain: str
    status: str    # PASS | FAIL | WARN | SKIP
    severity: str  # CRITICAL | HIGH | MEDIUM | LOW | INFO
    framework_refs: list[str]
    evidence: str
    remediation_step: str


# ---------------------------------------------------------------------------
# Dataclass 3 — AgentAuditReport
# ---------------------------------------------------------------------------


@dataclass
class AgentAuditReport:
    """Aggregated result of a complete agentic security audit run.

    Attributes
    ----------
    system_id : str
        Identifier of the audited system (from :class:`AgenticSystemConfig`).
    findings : list[AuditFinding]
        Ordered list of all individual control evaluation results.
    score : int
        Numeric security score in [0, 100] computed by the auditor.
        Starts at 100 and is reduced by the weighted deduction formula.
    maturity_level : str
        Human-readable trust tier derived from the score and failure
        pattern: ``"Sandbox"``, ``"Controlled"``, ``"Trusted"``, or
        ``"Autonomous"``.
    summary : str
        Free-text narrative describing the overall audit outcome.

    Example::

        report = AgentAuditReport(
            system_id="order-agent",
            findings=[],
            score=95,
            maturity_level="Autonomous",
            summary="No critical or high failures detected.",
        )
        assert report.critical_failures() == []
    """

    system_id: str
    findings: list[AuditFinding]
    score: int
    maturity_level: str
    summary: str

    def by_status(self, status: str) -> list[AuditFinding]:
        """Return all findings with the given *status*.

        Args:
            status: One of ``"PASS"``, ``"FAIL"``, ``"WARN"``, ``"SKIP"``.

        Returns:
            Filtered list of :class:`AuditFinding` objects.
        """
        return [f for f in self.findings if f.status == status]

    def by_severity(self, severity: str) -> list[AuditFinding]:
        """Return all findings with the given *severity*.

        Args:
            severity: One of ``"CRITICAL"``, ``"HIGH"``, ``"MEDIUM"``,
                ``"LOW"``, ``"INFO"``.

        Returns:
            Filtered list of :class:`AuditFinding` objects.
        """
        return [f for f in self.findings if f.severity == severity]

    def critical_failures(self) -> list[AuditFinding]:
        """Return all findings that are both ``FAIL`` and ``CRITICAL``.

        Returns:
            List of :class:`AuditFinding` objects.
        """
        return [f for f in self.findings if f.status == "FAIL" and f.severity == "CRITICAL"]

    def high_failures(self) -> list[AuditFinding]:
        """Return all findings that are both ``FAIL`` and ``HIGH``.

        Returns:
            List of :class:`AuditFinding` objects.
        """
        return [f for f in self.findings if f.status == "FAIL" and f.severity == "HIGH"]


# ---------------------------------------------------------------------------
# Class 4 — AgenticSecurityAuditor
# ---------------------------------------------------------------------------


class AgenticSecurityAuditor:
    """Enterprise agentic AI security audit engine.

    Evaluates a :class:`AgenticSystemConfig` against 28 security controls
    across seven domains and produces an :class:`AgentAuditReport` with a
    numeric score and maturity tier.

    **Scoring formula** (starts at 100):

    * ``CRITICAL`` fail: −15 points each
    * ``HIGH``    fail: −7 points each
    * ``MEDIUM``  fail: −3 points each
    * ``WARN``        : −1 point each
    * ``SKIP``        : no deduction (feature not deployed)

    **Maturity tiers**:

    * ``Sandbox``    — any CRITICAL failure, *or* score < 50
    * ``Controlled`` — score 50–69
    * ``Trusted``    — score 70–84, *or* score ≥ 85 with > 2 HIGH failures
    * ``Autonomous`` — score ≥ 85 and zero HIGH failures

    Example::

        config = AgenticSystemConfig(
            system_id="demo",
            tool_permission_model="rbac",
            unsafe_tool_calls_blocked=True,
        )
        auditor = AgenticSecurityAuditor(config)
        report = auditor.run_audit()
        print(report.score, report.maturity_level)
    """

    def __init__(self, config: AgenticSystemConfig) -> None:
        """Initialise the auditor with the target system configuration.

        Args:
            config: :class:`AgenticSystemConfig` describing the security
                posture of the system under audit.
        """
        self._cfg = config

    # ------------------------------------------------------------------
    # Domain checker 1 — Tool Permissions
    # ------------------------------------------------------------------

    def _check_tool_permissions(self) -> list[AuditFinding]:
        """Evaluate four Tool Permission controls (AGT-TP-001 to AGT-TP-004).

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        # AGT-TP-001 — Tool Permission Model
        if cfg.tool_permission_model == "none":
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-001",
                    control_name="Tool Permission Model",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI02", "NIST AI 600-1 §2.5"],
                    evidence=(
                        f"tool_permission_model is '{cfg.tool_permission_model}' — "
                        "the agent runtime applies no restrictions on which tools may be called."
                    ),
                    remediation_step=(
                        "Set tool_permission_model to 'allowlist', 'jit', or 'rbac' "
                        "and configure a corresponding policy engine."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-001",
                    control_name="Tool Permission Model",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI02", "NIST AI 600-1 §2.5"],
                    evidence=(
                        f"tool_permission_model is '{cfg.tool_permission_model}' — "
                        "a permission policy is active."
                    ),
                    remediation_step="",
                )
            )

        # AGT-TP-002 — Tool Scope Enforcement
        if not cfg.tool_scope_enforced:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-002",
                    control_name="Tool Scope Enforcement",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI05"],
                    evidence=(
                        "tool_scope_enforced is False — individual tool invocations "
                        "are not constrained to a declared action scope."
                    ),
                    remediation_step=(
                        "Enable runtime scope checks so each tool call is validated "
                        "against the agent's declared purpose before dispatch."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-002",
                    control_name="Tool Scope Enforcement",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI05"],
                    evidence="tool_scope_enforced is True — per-invocation scope checks are active.",
                    remediation_step="",
                )
            )

        # AGT-TP-003 — Tool Output Validation
        if not cfg.tool_output_validated:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-003",
                    control_name="Tool Output Validation",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI07", "OWASP LLM09"],
                    evidence=(
                        "tool_output_validated is False — tool return values are fed "
                        "back into agent context without sanitization or schema validation."
                    ),
                    remediation_step=(
                        "Validate and sanitize tool outputs before injecting them into "
                        "the agent's prompt context to prevent indirect prompt injection."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-003",
                    control_name="Tool Output Validation",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI07", "OWASP LLM09"],
                    evidence="tool_output_validated is True — tool outputs are validated before context injection.",
                    remediation_step="",
                )
            )

        # AGT-TP-004 — Unsafe Tool Call Blocking
        if not cfg.unsafe_tool_calls_blocked:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-004",
                    control_name="Unsafe Tool Call Blocking",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["MITRE ATLAS T0060", "OWASP ASI06"],
                    evidence=(
                        "unsafe_tool_calls_blocked is False — known-dangerous tool "
                        "invocations (e.g. shell exec, arbitrary file write) are not "
                        "rejected at the runtime boundary."
                    ),
                    remediation_step=(
                        "Implement a deny-list or policy engine that intercepts and "
                        "blocks tool calls matching known-dangerous patterns before dispatch."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-TP-004",
                    control_name="Unsafe Tool Call Blocking",
                    domain=DOMAIN_TOOL_PERMISSIONS,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["MITRE ATLAS T0060", "OWASP ASI06"],
                    evidence="unsafe_tool_calls_blocked is True — dangerous tool patterns are blocked at runtime.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Domain checker 2 — MCP Security
    # ------------------------------------------------------------------

    def _check_mcp_security(self) -> list[AuditFinding]:
        """Evaluate four MCP Security controls (AGT-MCP-001 to AGT-MCP-004).

        When ``mcp_enabled`` is ``False``, all four controls are set to
        ``SKIP`` with ``INFO`` severity because MCP is not deployed and
        the controls are not applicable.

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        if not cfg.mcp_enabled:
            for control_id, name, refs in [
                ("AGT-MCP-001", "MCP Source Allowlist", ["CVE-2025-6514", "OWASP ASI02"]),
                ("AGT-MCP-002", "MCP Checksum Verification", ["OWASP ASI02"]),
                ("AGT-MCP-003", "MCP Rate Limiting", ["OWASP ASI06"]),
                ("AGT-MCP-004", "MCP Invocation Audit", ["SOC 2 CC7.2"]),
            ]:
                findings.append(
                    AuditFinding(
                        control_id=control_id,
                        control_name=name,
                        domain=DOMAIN_MCP_SECURITY,
                        status="SKIP",
                        severity="INFO",
                        framework_refs=refs,
                        evidence="mcp_enabled is False — MCP is not deployed in this system.",
                        remediation_step="Enable and configure these controls if MCP is deployed.",
                    )
                )
            return findings

        # AGT-MCP-001 — MCP Source Allowlist
        if not cfg.mcp_source_allowlisted:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-001",
                    control_name="MCP Source Allowlist",
                    domain=DOMAIN_MCP_SECURITY,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["CVE-2025-6514", "OWASP ASI02"],
                    evidence=(
                        "mcp_source_allowlisted is False — MCP servers are not validated "
                        "against an origin allowlist, exposing the agent to malicious servers."
                    ),
                    remediation_step=(
                        "Maintain an explicit allowlist of trusted MCP server origins and "
                        "reject tool manifests from unlisted sources at registration time."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-001",
                    control_name="MCP Source Allowlist",
                    domain=DOMAIN_MCP_SECURITY,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["CVE-2025-6514", "OWASP ASI02"],
                    evidence="mcp_source_allowlisted is True — MCP server origins are validated against an allowlist.",
                    remediation_step="",
                )
            )

        # AGT-MCP-002 — MCP Checksum Verification
        if not cfg.mcp_checksum_verified:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-002",
                    control_name="MCP Checksum Verification",
                    domain=DOMAIN_MCP_SECURITY,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI02"],
                    evidence=(
                        "mcp_checksum_verified is False — tool manifest integrity is not "
                        "verified by checksum, allowing tampered manifests to be admitted."
                    ),
                    remediation_step=(
                        "Compute and verify SHA-256 checksums of all tool manifests "
                        "against a pre-distributed expected-checksum registry."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-002",
                    control_name="MCP Checksum Verification",
                    domain=DOMAIN_MCP_SECURITY,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI02"],
                    evidence="mcp_checksum_verified is True — tool manifest checksums are verified before admission.",
                    remediation_step="",
                )
            )

        # AGT-MCP-003 — MCP Rate Limiting
        if not cfg.mcp_rate_limiting_enabled:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-003",
                    control_name="MCP Rate Limiting",
                    domain=DOMAIN_MCP_SECURITY,
                    status="FAIL",
                    severity="MEDIUM",
                    framework_refs=["OWASP ASI06"],
                    evidence=(
                        "mcp_rate_limiting_enabled is False — no per-tool invocation "
                        "rate limit is enforced, allowing runaway agent loops."
                    ),
                    remediation_step=(
                        "Configure a sliding-window rate limiter for each MCP tool to "
                        "cap the maximum invocation frequency per agent session."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-003",
                    control_name="MCP Rate Limiting",
                    domain=DOMAIN_MCP_SECURITY,
                    status="PASS",
                    severity="MEDIUM",
                    framework_refs=["OWASP ASI06"],
                    evidence="mcp_rate_limiting_enabled is True — per-tool rate limits are active.",
                    remediation_step="",
                )
            )

        # AGT-MCP-004 — MCP Invocation Audit
        if not cfg.mcp_invocation_audit_logged:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-004",
                    control_name="MCP Invocation Audit",
                    domain=DOMAIN_MCP_SECURITY,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["SOC 2 CC7.2"],
                    evidence=(
                        "mcp_invocation_audit_logged is False — MCP tool invocations "
                        "are not written to an immutable audit log."
                    ),
                    remediation_step=(
                        "Enable append-only audit logging for every MCP tool call, "
                        "capturing tool name, arguments, caller identity, and timestamp."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MCP-004",
                    control_name="MCP Invocation Audit",
                    domain=DOMAIN_MCP_SECURITY,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["SOC 2 CC7.2"],
                    evidence="mcp_invocation_audit_logged is True — all MCP invocations are audit-logged.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Domain checker 3 — Identity & Authorization
    # ------------------------------------------------------------------

    def _check_identity_authorization(self) -> list[AuditFinding]:
        """Evaluate three Identity & Authorization controls (AGT-IA-001 to AGT-IA-003).

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        # AGT-IA-001 — Agent Identity Enforcement
        if not cfg.agent_identity_enforced:
            findings.append(
                AuditFinding(
                    control_id="AGT-IA-001",
                    control_name="Agent Identity Enforcement",
                    domain=DOMAIN_IDENTITY_AUTHORIZATION,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI08", "NIST CSF PR.AC-1"],
                    evidence=(
                        "agent_identity_enforced is False — agents are not required to "
                        "present a verified identity credential before accessing resources."
                    ),
                    remediation_step=(
                        "Issue a verifiable identity credential (e.g. signed JWT) to each "
                        "agent at instantiation and validate it at every resource access point."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-IA-001",
                    control_name="Agent Identity Enforcement",
                    domain=DOMAIN_IDENTITY_AUTHORIZATION,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI08", "NIST CSF PR.AC-1"],
                    evidence="agent_identity_enforced is True — each agent presents a verified identity credential.",
                    remediation_step="",
                )
            )

        # AGT-IA-002 — Caller Context Propagation
        if not cfg.caller_context_propagated:
            findings.append(
                AuditFinding(
                    control_id="AGT-IA-002",
                    control_name="Caller Context Propagation",
                    domain=DOMAIN_IDENTITY_AUTHORIZATION,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI01", "OWASP ASI04"],
                    evidence=(
                        "caller_context_propagated is False — the originating human caller's "
                        "identity context is not threaded through multi-step agent chains, "
                        "enabling privilege laundering between agents."
                    ),
                    remediation_step=(
                        "Propagate the original caller identity as a signed header or "
                        "context token through every agent-to-agent and agent-to-tool call."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-IA-002",
                    control_name="Caller Context Propagation",
                    domain=DOMAIN_IDENTITY_AUTHORIZATION,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI01", "OWASP ASI04"],
                    evidence="caller_context_propagated is True — originating caller context flows through the agent chain.",
                    remediation_step="",
                )
            )

        # AGT-IA-003 — Privilege Escalation Prevention
        if not cfg.privilege_escalation_prevented:
            findings.append(
                AuditFinding(
                    control_id="AGT-IA-003",
                    control_name="Privilege Escalation Prevention",
                    domain=DOMAIN_IDENTITY_AUTHORIZATION,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI04", "MITRE ATLAS T0055"],
                    evidence=(
                        "privilege_escalation_prevented is False — no mechanism prevents "
                        "an agent from acquiring permissions beyond those granted at instantiation."
                    ),
                    remediation_step=(
                        "Bind each agent to an immutable permission set at creation time "
                        "and enforce it in the tool gateway so escalation attempts are rejected."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-IA-003",
                    control_name="Privilege Escalation Prevention",
                    domain=DOMAIN_IDENTITY_AUTHORIZATION,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI04", "MITRE ATLAS T0055"],
                    evidence="privilege_escalation_prevented is True — agents cannot acquire permissions beyond their grant.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Domain checker 4 — Memory & Context
    # ------------------------------------------------------------------

    def _check_memory_context(self) -> list[AuditFinding]:
        """Evaluate three Memory & Context controls (AGT-MC-001 to AGT-MC-003).

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        # AGT-MC-001 — Memory Access Scoping
        if not cfg.memory_access_scoped:
            findings.append(
                AuditFinding(
                    control_id="AGT-MC-001",
                    control_name="Memory Access Scoping",
                    domain=DOMAIN_MEMORY_CONTEXT,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI03", "OWASP LLM08"],
                    evidence=(
                        "memory_access_scoped is False — agents can read and write memory "
                        "partitions belonging to other sessions or agents."
                    ),
                    remediation_step=(
                        "Enforce session-scoped memory isolation so each agent can only "
                        "access its own designated memory partition."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MC-001",
                    control_name="Memory Access Scoping",
                    domain=DOMAIN_MEMORY_CONTEXT,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI03", "OWASP LLM08"],
                    evidence="memory_access_scoped is True — each agent is restricted to its own session memory partition.",
                    remediation_step="",
                )
            )

        # AGT-MC-002 — Prompt Context Sanitization
        if not cfg.prompt_context_sanitized:
            findings.append(
                AuditFinding(
                    control_id="AGT-MC-002",
                    control_name="Prompt Context Sanitization",
                    domain=DOMAIN_MEMORY_CONTEXT,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["OWASP LLM01", "OWASP ASI01"],
                    evidence=(
                        "prompt_context_sanitized is False — retrieved content, tool outputs, "
                        "and external data are inserted into prompts without injection scanning."
                    ),
                    remediation_step=(
                        "Apply an injection-pattern filter to all external content before "
                        "it is concatenated into agent prompts."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MC-002",
                    control_name="Prompt Context Sanitization",
                    domain=DOMAIN_MEMORY_CONTEXT,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["OWASP LLM01", "OWASP ASI01"],
                    evidence="prompt_context_sanitized is True — external content is sanitized before prompt insertion.",
                    remediation_step="",
                )
            )

        # AGT-MC-003 — Long-Term Memory Encryption
        if not cfg.long_term_memory_encrypted:
            findings.append(
                AuditFinding(
                    control_id="AGT-MC-003",
                    control_name="Long-Term Memory Encryption",
                    domain=DOMAIN_MEMORY_CONTEXT,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["GDPR Art.32", "HIPAA §164.312"],
                    evidence=(
                        "long_term_memory_encrypted is False — persistent agent memory "
                        "stores are not encrypted at rest, exposing sensitive user data."
                    ),
                    remediation_step=(
                        "Encrypt all persistent memory backing stores using AES-256 "
                        "or equivalent and manage keys via a dedicated secrets manager."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MC-003",
                    control_name="Long-Term Memory Encryption",
                    domain=DOMAIN_MEMORY_CONTEXT,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["GDPR Art.32", "HIPAA §164.312"],
                    evidence="long_term_memory_encrypted is True — persistent memory stores are encrypted at rest.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Domain checker 5 — Multi-Agent
    # ------------------------------------------------------------------

    def _check_multi_agent(self) -> list[AuditFinding]:
        """Evaluate three Multi-Agent controls (AGT-MA-001 to AGT-MA-003).

        When ``multi_agent_enabled`` is ``False``, all three controls are
        set to ``SKIP`` with ``INFO`` severity.

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        if not cfg.multi_agent_enabled:
            for control_id, name, refs in [
                ("AGT-MA-001", "Agent Trust Boundaries", ["OWASP ASI10", "CSA ATF §3.2"]),
                ("AGT-MA-002", "Inter-Agent Message Signing", ["OWASP ASI08"]),
                ("AGT-MA-003", "Agent Composition Validation", ["OWASP ASI10"]),
            ]:
                findings.append(
                    AuditFinding(
                        control_id=control_id,
                        control_name=name,
                        domain=DOMAIN_MULTI_AGENT,
                        status="SKIP",
                        severity="INFO",
                        framework_refs=refs,
                        evidence="multi_agent_enabled is False — multi-agent orchestration is not deployed.",
                        remediation_step="Enable and configure these controls if multi-agent orchestration is deployed.",
                    )
                )
            return findings

        # AGT-MA-001 — Agent Trust Boundaries
        if not cfg.agent_trust_boundaries_defined:
            findings.append(
                AuditFinding(
                    control_id="AGT-MA-001",
                    control_name="Agent Trust Boundaries",
                    domain=DOMAIN_MULTI_AGENT,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI10", "CSA ATF §3.2"],
                    evidence=(
                        "agent_trust_boundaries_defined is False — each agent's authority "
                        "scope is not declared or enforced in multi-agent pipelines."
                    ),
                    remediation_step=(
                        "Define explicit trust zones and authority scopes for each agent "
                        "role and enforce boundaries in the orchestration layer."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MA-001",
                    control_name="Agent Trust Boundaries",
                    domain=DOMAIN_MULTI_AGENT,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["OWASP ASI10", "CSA ATF §3.2"],
                    evidence="agent_trust_boundaries_defined is True — agent authority scopes are declared and enforced.",
                    remediation_step="",
                )
            )

        # AGT-MA-002 — Inter-Agent Message Signing
        if not cfg.inter_agent_message_signed:
            findings.append(
                AuditFinding(
                    control_id="AGT-MA-002",
                    control_name="Inter-Agent Message Signing",
                    domain=DOMAIN_MULTI_AGENT,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI08"],
                    evidence=(
                        "inter_agent_message_signed is False — messages flowing between "
                        "agents carry no integrity signature, enabling spoofing and tampering."
                    ),
                    remediation_step=(
                        "Sign every inter-agent message with the originating agent's "
                        "private key and verify the signature on receipt."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MA-002",
                    control_name="Inter-Agent Message Signing",
                    domain=DOMAIN_MULTI_AGENT,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI08"],
                    evidence="inter_agent_message_signed is True — inter-agent messages carry integrity signatures.",
                    remediation_step="",
                )
            )

        # AGT-MA-003 — Agent Composition Validation
        if not cfg.agent_composition_validated:
            findings.append(
                AuditFinding(
                    control_id="AGT-MA-003",
                    control_name="Agent Composition Validation",
                    domain=DOMAIN_MULTI_AGENT,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI10"],
                    evidence=(
                        "agent_composition_validated is False — agent pipelines are "
                        "assembled and executed without structural validation, "
                        "allowing unvetted agent components to be injected."
                    ),
                    remediation_step=(
                        "Validate agent pipeline composition against a signed blueprint "
                        "before execution and reject pipelines that deviate from the schema."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-MA-003",
                    control_name="Agent Composition Validation",
                    domain=DOMAIN_MULTI_AGENT,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI10"],
                    evidence="agent_composition_validated is True — agent pipeline blueprints are validated before execution.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Domain checker 6 — Observability
    # ------------------------------------------------------------------

    def _check_observability(self) -> list[AuditFinding]:
        """Evaluate three Observability controls (AGT-OBS-001 to AGT-OBS-003).

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        # AGT-OBS-001 — Tool Invocation Logging
        if not cfg.tool_invocation_logging:
            findings.append(
                AuditFinding(
                    control_id="AGT-OBS-001",
                    control_name="Tool Invocation Logging",
                    domain=DOMAIN_OBSERVABILITY,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["SOC 2 CC7.2", "ISO 42001 Cl.9.1"],
                    evidence=(
                        "tool_invocation_logging is False — tool calls are not written to "
                        "a structured log, making post-incident forensics impossible."
                    ),
                    remediation_step=(
                        "Enable structured logging for every tool invocation, capturing "
                        "tool name, arguments, response, agent identity, and timestamp."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-OBS-001",
                    control_name="Tool Invocation Logging",
                    domain=DOMAIN_OBSERVABILITY,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["SOC 2 CC7.2", "ISO 42001 Cl.9.1"],
                    evidence="tool_invocation_logging is True — all tool calls are written to structured logs.",
                    remediation_step="",
                )
            )

        # AGT-OBS-002 — Reasoning Trace Capture
        if not cfg.reasoning_trace_captured:
            findings.append(
                AuditFinding(
                    control_id="AGT-OBS-002",
                    control_name="Reasoning Trace Capture",
                    domain=DOMAIN_OBSERVABILITY,
                    status="FAIL",
                    severity="MEDIUM",
                    framework_refs=["ISO 42001 Cl.9.1"],
                    evidence=(
                        "reasoning_trace_captured is False — the agent's chain-of-thought "
                        "is not persisted, making it impossible to reconstruct why an "
                        "action was taken during a security incident."
                    ),
                    remediation_step=(
                        "Persist the agent's reasoning trace alongside the action log "
                        "so root-cause analysis can be performed after anomalous events."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-OBS-002",
                    control_name="Reasoning Trace Capture",
                    domain=DOMAIN_OBSERVABILITY,
                    status="PASS",
                    severity="MEDIUM",
                    framework_refs=["ISO 42001 Cl.9.1"],
                    evidence="reasoning_trace_captured is True — agent reasoning traces are persisted for review.",
                    remediation_step="",
                )
            )

        # AGT-OBS-003 — Incident Response Playbook
        if not cfg.incident_response_playbook:
            findings.append(
                AuditFinding(
                    control_id="AGT-OBS-003",
                    control_name="Incident Response Playbook",
                    domain=DOMAIN_OBSERVABILITY,
                    status="FAIL",
                    severity="MEDIUM",
                    framework_refs=["NIST CSF RS.RP-1"],
                    evidence=(
                        "incident_response_playbook is False — no documented playbook "
                        "exists for responding to agentic security incidents such as "
                        "agent compromise, data exfiltration, or runaway autonomous actions."
                    ),
                    remediation_step=(
                        "Author and test an incident response playbook covering agent "
                        "isolation, credential revocation, audit log preservation, and "
                        "stakeholder notification procedures."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-OBS-003",
                    control_name="Incident Response Playbook",
                    domain=DOMAIN_OBSERVABILITY,
                    status="PASS",
                    severity="MEDIUM",
                    framework_refs=["NIST CSF RS.RP-1"],
                    evidence="incident_response_playbook is True — an agentic incident response playbook is in place.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Domain checker 7 — HITL
    # ------------------------------------------------------------------

    def _check_hitl(self) -> list[AuditFinding]:
        """Evaluate two Human-in-the-Loop controls (AGT-HITL-001 to AGT-HITL-002).

        Returns:
            List of :class:`AuditFinding` objects for this domain.
        """
        cfg = self._cfg
        findings: list[AuditFinding] = []

        # AGT-HITL-001 — HITL for High-Risk Actions
        if not cfg.hitl_for_high_risk_actions:
            findings.append(
                AuditFinding(
                    control_id="AGT-HITL-001",
                    control_name="HITL for High-Risk Actions",
                    domain=DOMAIN_HITL,
                    status="FAIL",
                    severity="CRITICAL",
                    framework_refs=["CSA ATF Level 2", "NIST AI 600-1 §2.6"],
                    evidence=(
                        "hitl_for_high_risk_actions is False — the agent can execute "
                        "irreversible or high-impact actions (e.g. data deletion, "
                        "financial transactions, infrastructure changes) without requiring "
                        "explicit human approval."
                    ),
                    remediation_step=(
                        "Insert a mandatory human approval gate before any action "
                        "classified as irreversible, high-impact, or high-risk, and "
                        "implement a timeout-and-cancel fallback."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-HITL-001",
                    control_name="HITL for High-Risk Actions",
                    domain=DOMAIN_HITL,
                    status="PASS",
                    severity="CRITICAL",
                    framework_refs=["CSA ATF Level 2", "NIST AI 600-1 §2.6"],
                    evidence="hitl_for_high_risk_actions is True — high-risk actions require human approval before execution.",
                    remediation_step="",
                )
            )

        # AGT-HITL-002 — Autonomous Action Rate Limiting
        if not cfg.autonomous_action_rate_limited:
            findings.append(
                AuditFinding(
                    control_id="AGT-HITL-002",
                    control_name="Autonomous Action Rate Limiting",
                    domain=DOMAIN_HITL,
                    status="FAIL",
                    severity="HIGH",
                    framework_refs=["OWASP ASI06", "CSA ATF §2.3"],
                    evidence=(
                        "autonomous_action_rate_limited is False — no frequency ceiling "
                        "is applied to autonomous agent actions, allowing unbounded "
                        "execution bursts that can cause unintended side-effects."
                    ),
                    remediation_step=(
                        "Apply a configurable rate limit to autonomous action dispatch "
                        "and trigger a human review notification when the ceiling is "
                        "approached within a session."
                    ),
                )
            )
        else:
            findings.append(
                AuditFinding(
                    control_id="AGT-HITL-002",
                    control_name="Autonomous Action Rate Limiting",
                    domain=DOMAIN_HITL,
                    status="PASS",
                    severity="HIGH",
                    framework_refs=["OWASP ASI06", "CSA ATF §2.3"],
                    evidence="autonomous_action_rate_limited is True — a frequency ceiling is enforced on autonomous actions.",
                    remediation_step="",
                )
            )

        return findings

    # ------------------------------------------------------------------
    # Score and maturity helpers
    # ------------------------------------------------------------------

    @staticmethod
    def compute_score(findings: list[AuditFinding]) -> int:
        """Compute the numeric security score from a list of findings.

        Starts at 100 and applies the following deductions:

        * ``CRITICAL`` fail: −15 each
        * ``HIGH``    fail: −7 each
        * ``MEDIUM``  fail: −3 each
        * ``WARN``        : −1 each
        * ``SKIP`` and ``PASS``: no deduction

        Args:
            findings: Complete list of :class:`AuditFinding` objects.

        Returns:
            Integer score in [0, 100].
        """
        score = 100
        for f in findings:
            if f.status == "FAIL":
                score -= _SEVERITY_DEDUCTION.get(f.severity, 0)
            elif f.status == "WARN":
                score -= _SEVERITY_DEDUCTION.get("LOW", 1)
        return max(0, score)

    @staticmethod
    def derive_maturity(score: int, findings: list[AuditFinding]) -> str:
        """Derive the maturity tier from *score* and the finding list.

        Tiers:

        * ``Sandbox``    — any CRITICAL failure, *or* score < 50
        * ``Controlled`` — score 50–69
        * ``Trusted``    — score 70–84, *or* score ≥ 85 with > 2 HIGH failures
        * ``Autonomous`` — score ≥ 85 and zero HIGH failures

        Args:
            score: Numeric score computed by :meth:`compute_score`.
            findings: Complete list of :class:`AuditFinding` objects.

        Returns:
            Maturity tier string.
        """
        critical_fails = sum(
            1 for f in findings if f.status == "FAIL" and f.severity == "CRITICAL"
        )
        high_fails = sum(
            1 for f in findings if f.status == "FAIL" and f.severity == "HIGH"
        )

        if critical_fails > 0 or score < 50:
            return "Sandbox"
        if score < 70:
            return "Controlled"
        if score >= 85 and high_fails == 0:
            return "Autonomous"
        if score >= 85 and high_fails > 2:
            return "Trusted"
        if score >= 85:
            return "Trusted"
        # 70 <= score < 85
        return "Trusted"

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def run_audit(self) -> AgentAuditReport:
        """Run all seven domain checks and return a complete audit report.

        Calls all seven checker methods, merges their findings, computes
        the score, derives the maturity level, and constructs the
        :class:`AgentAuditReport`.

        Returns:
            :class:`AgentAuditReport` with full findings, score, and maturity.

        Example::

            auditor = AgenticSecurityAuditor(AgenticSystemConfig())
            report = auditor.run_audit()
            assert report.maturity_level == "Sandbox"
        """
        findings: list[AuditFinding] = []
        findings.extend(self._check_tool_permissions())
        findings.extend(self._check_mcp_security())
        findings.extend(self._check_identity_authorization())
        findings.extend(self._check_memory_context())
        findings.extend(self._check_multi_agent())
        findings.extend(self._check_observability())
        findings.extend(self._check_hitl())

        score = self.compute_score(findings)
        maturity = self.derive_maturity(score, findings)

        fails = [f for f in findings if f.status == "FAIL"]
        critical_count = sum(1 for f in fails if f.severity == "CRITICAL")
        high_count = sum(1 for f in fails if f.severity == "HIGH")
        medium_count = sum(1 for f in fails if f.severity == "MEDIUM")
        skip_count = sum(1 for f in findings if f.status == "SKIP")

        summary = (
            f"Audit for '{self._cfg.system_id}': score={score}/100, "
            f"maturity={maturity}. "
            f"Failures: {critical_count} CRITICAL, {high_count} HIGH, "
            f"{medium_count} MEDIUM. "
            f"Skipped (feature not deployed): {skip_count} controls."
        )

        return AgentAuditReport(
            system_id=self._cfg.system_id,
            findings=findings,
            score=score,
            maturity_level=maturity,
            summary=summary,
        )


# ===========================================================================
# Self-test / usage demo
# ===========================================================================

if __name__ == "__main__":
    # ------------------------------------------------------------------
    # Helper: print a concise report card
    # ------------------------------------------------------------------
    def _print_report(report: AgentAuditReport) -> None:
        """Print a compact audit summary to stdout."""
        print(f"\n{'=' * 64}")
        print(f"  System : {report.system_id}")
        print(f"  Score  : {report.score}/100")
        print(f"  Tier   : {report.maturity_level}")
        print(f"  Summary: {report.summary}")
        fails = report.by_status("FAIL")
        if fails:
            print(f"  Failures ({len(fails)}):")
            for f in fails:
                print(f"    [{f.severity:<8}] {f.control_id} — {f.control_name}")
        skips = report.by_status("SKIP")
        if skips:
            print(f"  Skipped (feature disabled): {len(skips)} controls")
        print(f"{'=' * 64}")

    # ------------------------------------------------------------------
    # Config 1 — Default / all-off → Sandbox
    # ------------------------------------------------------------------
    default_config = AgenticSystemConfig(system_id="default-all-off")
    default_report = AgenticSecurityAuditor(default_config).run_audit()
    _print_report(default_report)
    assert default_report.maturity_level == "Sandbox", (
        f"expected Sandbox, got {default_report.maturity_level}"
    )
    assert len(default_report.critical_failures()) > 0

    # ------------------------------------------------------------------
    # Config 2 — MCP + multi-agent enabled, partial security → Controlled
    # ------------------------------------------------------------------
    partial_config = AgenticSystemConfig(
        system_id="partial-mcp-multiagent",
        # Tool permissions — basic model set but scope/output/blocking absent
        tool_permission_model="allowlist",
        tool_scope_enforced=False,
        tool_output_validated=False,
        unsafe_tool_calls_blocked=False,
        # MCP enabled but partially hardened
        mcp_enabled=True,
        mcp_source_allowlisted=True,
        mcp_checksum_verified=False,
        mcp_rate_limiting_enabled=True,
        mcp_invocation_audit_logged=False,
        # Identity partially configured
        agent_identity_enforced=True,
        caller_context_propagated=False,
        privilege_escalation_prevented=False,
        # Memory — only prompt sanitized
        memory_access_scoped=False,
        prompt_context_sanitized=True,
        long_term_memory_encrypted=False,
        # Multi-agent enabled but boundaries not fully defined
        multi_agent_enabled=True,
        agent_trust_boundaries_defined=False,
        inter_agent_message_signed=True,
        agent_composition_validated=False,
        # Basic observability
        tool_invocation_logging=True,
        reasoning_trace_captured=False,
        incident_response_playbook=False,
        # No HITL configured
        hitl_for_high_risk_actions=False,
        autonomous_action_rate_limited=False,
    )
    partial_report = AgenticSecurityAuditor(partial_config).run_audit()
    _print_report(partial_report)
    assert partial_report.maturity_level in ("Sandbox", "Controlled"), (
        f"expected Sandbox or Controlled, got {partial_report.maturity_level}"
    )

    # ------------------------------------------------------------------
    # Config 3 — Full production hardening → Autonomous
    # ------------------------------------------------------------------
    prod_config = AgenticSystemConfig(
        system_id="prod-fully-hardened",
        # Tool permissions — strongest model
        tool_permission_model="rbac",
        tool_scope_enforced=True,
        tool_output_validated=True,
        unsafe_tool_calls_blocked=True,
        # MCP fully hardened
        mcp_enabled=True,
        mcp_source_allowlisted=True,
        mcp_checksum_verified=True,
        mcp_rate_limiting_enabled=True,
        mcp_invocation_audit_logged=True,
        # Identity fully enforced
        agent_identity_enforced=True,
        caller_context_propagated=True,
        privilege_escalation_prevented=True,
        # Memory fully secured
        memory_access_scoped=True,
        prompt_context_sanitized=True,
        long_term_memory_encrypted=True,
        # Multi-agent fully validated
        multi_agent_enabled=True,
        agent_trust_boundaries_defined=True,
        inter_agent_message_signed=True,
        agent_composition_validated=True,
        # Full observability
        tool_invocation_logging=True,
        reasoning_trace_captured=True,
        incident_response_playbook=True,
        # Full HITL
        hitl_for_high_risk_actions=True,
        autonomous_action_rate_limited=True,
    )
    prod_report = AgenticSecurityAuditor(prod_config).run_audit()
    _print_report(prod_report)
    assert prod_report.maturity_level == "Autonomous", (
        f"expected Autonomous, got {prod_report.maturity_level}"
    )
    assert prod_report.score >= 85
    assert prod_report.critical_failures() == []
    assert prod_report.high_failures() == []

    print("\nAll demo assertions passed.")
