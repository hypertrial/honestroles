from __future__ import annotations

from dataclasses import dataclass
import hashlib

REQUIRED_TABLES: tuple[str, ...] = (
    "jobs_live",
    "job_features",
    "job_facets",
    "publish_batches",
    "feedback_events",
    "profile_weights",
    "profile_cache",
    "migration_history",
)

REQUIRED_FUNCTIONS: tuple[str, ...] = ("match_jobs_v1",)


@dataclass(frozen=True, slots=True)
class Migration:
    version: str
    sql: str

    @property
    def checksum(self) -> str:
        digest = hashlib.sha256()
        digest.update(self.sql.encode("utf-8"))
        return digest.hexdigest()


def migrations_for_schema(schema: str) -> tuple[Migration, ...]:
    return (
        Migration(version="0001_neon_agent_api_v1", sql=_migration_0001(schema)),
    )


def _migration_0001(schema: str) -> str:
    return f"""
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.migration_history (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {schema}.jobs_live (
    job_id TEXT PRIMARY KEY,
    source_job_id TEXT,
    id TEXT,
    title TEXT,
    company TEXT,
    location TEXT,
    work_mode TEXT,
    seniority TEXT,
    employment_type TEXT,
    remote BOOLEAN,
    description_text TEXT,
    description_html TEXT,
    skills TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    salary_min DOUBLE PRECISION,
    salary_max DOUBLE PRECISION,
    salary_currency TEXT,
    salary_interval TEXT,
    apply_url TEXT,
    posted_at TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ,
    source TEXT,
    source_ref TEXT,
    job_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_jobs_live_active ON {schema}.jobs_live (is_active);
CREATE INDEX IF NOT EXISTS idx_jobs_live_posted_at ON {schema}.jobs_live (posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_live_source_ref ON {schema}.jobs_live (source, source_ref);

CREATE TABLE IF NOT EXISTS {schema}.job_features (
    job_id TEXT PRIMARY KEY REFERENCES {schema}.jobs_live(job_id) ON DELETE CASCADE,
    title_tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    skill_tokens TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    location_text TEXT,
    work_mode TEXT,
    seniority TEXT,
    employment_type TEXT,
    salary_min DOUBLE PRECISION,
    salary_max DOUBLE PRECISION,
    quality_flags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    visa_no_sponsorship BOOLEAN NOT NULL DEFAULT FALSE,
    posted_at TIMESTAMPTZ,
    source_updated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS {schema}.job_facets (
    facet_name TEXT NOT NULL,
    facet_value TEXT NOT NULL,
    facet_count INTEGER NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (facet_name, facet_value)
);

CREATE TABLE IF NOT EXISTS {schema}.publish_batches (
    batch_id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    schema_version TEXT NOT NULL,
    jobs_parquet_hash TEXT,
    index_manifest_hash TEXT,
    policy_hash TEXT,
    require_quality_pass BOOLEAN NOT NULL DEFAULT TRUE,
    quality_gate_status TEXT NOT NULL,
    full_refresh BOOLEAN NOT NULL DEFAULT FALSE,
    inserted_count INTEGER NOT NULL DEFAULT 0,
    updated_count INTEGER NOT NULL DEFAULT 0,
    deactivated_count INTEGER NOT NULL DEFAULT 0,
    active_jobs INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS {schema}.feedback_events (
    id BIGSERIAL PRIMARY KEY,
    profile_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    event TEXT NOT NULL,
    meta JSONB NOT NULL DEFAULT '{{}}'::JSONB,
    event_hash TEXT NOT NULL UNIQUE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feedback_events_profile ON {schema}.feedback_events (profile_id, recorded_at DESC);

CREATE TABLE IF NOT EXISTS {schema}.profile_weights (
    profile_id TEXT PRIMARY KEY,
    multipliers JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS {schema}.profile_cache (
    profile_id TEXT PRIMARY KEY,
    profile JSONB NOT NULL,
    source TEXT NOT NULL DEFAULT 'api',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE OR REPLACE FUNCTION {schema}.match_jobs_v1(
    candidate JSONB,
    top_k INTEGER DEFAULT 25,
    include_excluded BOOLEAN DEFAULT FALSE,
    policy_override JSONB DEFAULT '{{}}'::JSONB
)
RETURNS TABLE (
    job_id TEXT,
    score DOUBLE PRECISION,
    match_reasons JSONB,
    required_missing_skills TEXT[],
    apply_url TEXT,
    posted_at TIMESTAMPTZ,
    source TEXT,
    quality_flags TEXT[],
    excluded BOOLEAN,
    exclude_reasons TEXT[]
)
LANGUAGE SQL
STABLE
AS $$
WITH candidate_raw AS (
    SELECT
        LOWER(COALESCE(candidate->>'profile_id', '')) AS profile_id,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate->'skills', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS skills,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate->'titles', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS titles,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate->'locations', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS locations,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate->'work_mode_preferences', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS work_mode_preferences,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate->'seniority_targets', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS seniority_targets,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate->'employment_type_preferences', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS employment_type_preferences,
        COALESCE((SELECT ARRAY_AGG(LOWER(value)) FROM JSONB_ARRAY_ELEMENTS_TEXT(COALESCE(candidate#>'{{visa_work_auth,authorized_locations}}', '[]'::JSONB)) AS value), ARRAY[]::TEXT[]) AS authorized_locations,
        CASE
            WHEN (candidate#>>'{{salary_targets,minimum}}') ~ '^-?[0-9]+(\\.[0-9]+)?$' THEN (candidate#>>'{{salary_targets,minimum}}')::DOUBLE PRECISION
            ELSE NULL
        END AS salary_floor,
        CASE
            WHEN LOWER(COALESCE(candidate#>>'{{visa_work_auth,requires_sponsorship}}', '')) = 'true' THEN TRUE
            WHEN LOWER(COALESCE(candidate#>>'{{visa_work_auth,requires_sponsorship}}', '')) = 'false' THEN FALSE
            ELSE NULL
        END AS requires_sponsorship,
        GREATEST(1, COALESCE(top_k, 25)) AS top_k,
        GREATEST(
            1,
            CASE
                WHEN COALESCE(policy_override->>'reason_limit', '') ~ '^[0-9]+$'
                THEN (policy_override->>'reason_limit')::INTEGER
                ELSE 3
            END
        ) AS reason_limit
),
base_weights AS (
    SELECT
        GREATEST(
            0.0,
            CASE
                WHEN COALESCE(policy_override#>>'{{weights,skills}}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (policy_override#>>'{{weights,skills}}')::DOUBLE PRECISION
                ELSE 0.35
            END
        ) AS skills,
        GREATEST(
            0.0,
            CASE
                WHEN COALESCE(policy_override#>>'{{weights,title}}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (policy_override#>>'{{weights,title}}')::DOUBLE PRECISION
                ELSE 0.20
            END
        ) AS title,
        GREATEST(
            0.0,
            CASE
                WHEN COALESCE(policy_override#>>'{{weights,location_work_mode}}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (policy_override#>>'{{weights,location_work_mode}}')::DOUBLE PRECISION
                ELSE 0.15
            END
        ) AS location_work_mode,
        GREATEST(
            0.0,
            CASE
                WHEN COALESCE(policy_override#>>'{{weights,seniority}}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (policy_override#>>'{{weights,seniority}}')::DOUBLE PRECISION
                ELSE 0.10
            END
        ) AS seniority,
        GREATEST(
            0.0,
            CASE
                WHEN COALESCE(policy_override#>>'{{weights,recency}}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (policy_override#>>'{{weights,recency}}')::DOUBLE PRECISION
                ELSE 0.10
            END
        ) AS recency,
        GREATEST(
            0.0,
            CASE
                WHEN COALESCE(policy_override#>>'{{weights,compensation}}', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                THEN (policy_override#>>'{{weights,compensation}}')::DOUBLE PRECISION
                ELSE 0.10
            END
        ) AS compensation
),
normalized_weights AS (
    SELECT
        CASE WHEN total > 0 THEN skills / total ELSE (1.0 / 6.0) END AS skills,
        CASE WHEN total > 0 THEN title / total ELSE (1.0 / 6.0) END AS title,
        CASE WHEN total > 0 THEN location_work_mode / total ELSE (1.0 / 6.0) END AS location_work_mode,
        CASE WHEN total > 0 THEN seniority / total ELSE (1.0 / 6.0) END AS seniority,
        CASE WHEN total > 0 THEN recency / total ELSE (1.0 / 6.0) END AS recency,
        CASE WHEN total > 0 THEN compensation / total ELSE (1.0 / 6.0) END AS compensation
    FROM (
        SELECT
            skills,
            title,
            location_work_mode,
            seniority,
            recency,
            compensation,
            (skills + title + location_work_mode + seniority + recency + compensation) AS total
        FROM base_weights
    ) t
),
profile_multipliers AS (
    SELECT
        COALESCE((pw.multipliers->>'skills')::DOUBLE PRECISION, 1.0) AS skills,
        COALESCE((pw.multipliers->>'title')::DOUBLE PRECISION, 1.0) AS title,
        COALESCE((pw.multipliers->>'location_work_mode')::DOUBLE PRECISION, 1.0) AS location_work_mode,
        COALESCE((pw.multipliers->>'seniority')::DOUBLE PRECISION, 1.0) AS seniority,
        COALESCE((pw.multipliers->>'recency')::DOUBLE PRECISION, 1.0) AS recency,
        COALESCE((pw.multipliers->>'compensation')::DOUBLE PRECISION, 1.0) AS compensation
    FROM candidate_raw c
    LEFT JOIN {schema}.profile_weights pw
        ON pw.profile_id = c.profile_id
        AND (pw.expires_at IS NULL OR pw.expires_at > NOW())
    LIMIT 1
),
weights AS (
    SELECT
        nw.skills * GREATEST(0.5, LEAST(1.5, COALESCE(pm.skills, 1.0))) AS skills,
        nw.title * GREATEST(0.5, LEAST(1.5, COALESCE(pm.title, 1.0))) AS title,
        nw.location_work_mode * GREATEST(0.5, LEAST(1.5, COALESCE(pm.location_work_mode, 1.0))) AS location_work_mode,
        nw.seniority * GREATEST(0.5, LEAST(1.5, COALESCE(pm.seniority, 1.0))) AS seniority,
        nw.recency * GREATEST(0.5, LEAST(1.5, COALESCE(pm.recency, 1.0))) AS recency,
        nw.compensation * GREATEST(0.5, LEAST(1.5, COALESCE(pm.compensation, 1.0))) AS compensation
    FROM normalized_weights nw
    LEFT JOIN profile_multipliers pm ON TRUE
),
base AS (
    SELECT
        jl.job_id,
        jl.title,
        jl.location,
        jl.work_mode,
        jl.seniority,
        jl.employment_type,
        jl.salary_min,
        jl.salary_max,
        jl.apply_url,
        jl.posted_at,
        jl.source,
        jf.skill_tokens,
        jf.title_tokens,
        jf.quality_flags,
        jf.visa_no_sponsorship
    FROM {schema}.jobs_live jl
    JOIN {schema}.job_features jf ON jf.job_id = jl.job_id
    WHERE jl.is_active = TRUE
),
eligibility AS (
    SELECT
        b.*,
        c.*,
        ARRAY_REMOVE(ARRAY[
            CASE
                WHEN CARDINALITY(c.locations) = 0 AND CARDINALITY(c.authorized_locations) = 0 THEN NULL
                WHEN EXISTS (
                    SELECT 1 FROM UNNEST(c.locations || c.authorized_locations) AS loc
                    WHERE loc <> '' AND POSITION(loc IN LOWER(COALESCE(b.location, ''))) > 0
                ) THEN NULL
                WHEN b.work_mode = 'remote' AND 'remote' = ANY(c.locations || c.authorized_locations) THEN NULL
                ELSE 'FILTER_LOCATION'
            END,
            CASE
                WHEN CARDINALITY(c.work_mode_preferences) = 0 THEN NULL
                WHEN LOWER(COALESCE(b.work_mode, 'unknown')) = ANY(c.work_mode_preferences) THEN NULL
                ELSE 'FILTER_WORK_MODE'
            END,
            CASE
                WHEN CARDINALITY(c.employment_type_preferences) = 0 THEN NULL
                WHEN LOWER(COALESCE(b.employment_type, 'unknown')) = ANY(c.employment_type_preferences) THEN NULL
                ELSE 'FILTER_EMPLOYMENT_TYPE'
            END,
            CASE
                WHEN c.salary_floor IS NULL THEN NULL
                WHEN GREATEST(COALESCE(b.salary_min, -1e18), COALESCE(b.salary_max, -1e18)) >= c.salary_floor THEN NULL
                ELSE 'FILTER_SALARY'
            END,
            CASE
                WHEN c.requires_sponsorship IS DISTINCT FROM TRUE THEN NULL
                WHEN COALESCE(b.visa_no_sponsorship, FALSE) = TRUE THEN 'FILTER_VISA'
                ELSE NULL
            END
        ], NULL) AS exclude_reasons
    FROM base b
    CROSS JOIN candidate_raw c
),
scored AS (
    SELECT
        e.job_id,
        e.apply_url,
        e.posted_at,
        e.source,
        e.quality_flags,
        e.exclude_reasons,
        e.reason_limit,
        e.skills AS candidate_skills,
        w.skills AS w_skills,
        w.title AS w_title,
        w.location_work_mode AS w_location_work_mode,
        w.seniority AS w_seniority,
        w.recency AS w_recency,
        w.compensation AS w_compensation,
        CASE
            WHEN CARDINALITY(e.skills) = 0 THEN 0.5
            ELSE (
                SELECT COUNT(*)::DOUBLE PRECISION FROM UNNEST(e.skills) AS skill
                WHERE skill = ANY(e.skill_tokens)
            ) / CARDINALITY(e.skills)
        END AS s_skills,
        CASE
            WHEN CARDINALITY(e.titles) = 0 THEN 0.5
            WHEN EXISTS (
                SELECT 1
                FROM UNNEST(e.titles) AS title
                CROSS JOIN LATERAL UNNEST(REGEXP_SPLIT_TO_ARRAY(REGEXP_REPLACE(title, '[^a-z0-9 ]', ' ', 'g'), '\\s+')) AS tok
                WHERE tok <> '' AND tok = ANY(e.title_tokens)
            ) THEN 1.0
            ELSE 0.0
        END AS s_title,
        CASE
            WHEN CARDINALITY(e.locations) = 0 AND CARDINALITY(e.authorized_locations) = 0 AND CARDINALITY(e.work_mode_preferences) = 0 THEN 0.5
            WHEN CARDINALITY(e.exclude_reasons) = 0 THEN 1.0
            WHEN ARRAY['FILTER_LOCATION']::TEXT[] <@ e.exclude_reasons AND ARRAY['FILTER_WORK_MODE']::TEXT[] <@ e.exclude_reasons THEN 0.0
            ELSE 0.5
        END AS s_location_work_mode,
        CASE
            WHEN CARDINALITY(e.seniority_targets) = 0 THEN 0.5
            WHEN LOWER(COALESCE(e.seniority, 'mid')) = ANY(e.seniority_targets) THEN 1.0
            ELSE 0.0
        END AS s_seniority,
        CASE
            WHEN e.posted_at IS NULL THEN 0.0
            ELSE GREATEST(0.0, LEAST(1.0, EXP(-LN(2.0) * (EXTRACT(EPOCH FROM (NOW() - e.posted_at)) / 86400.0 / 90.0))))
        END AS s_recency,
        CASE
            WHEN e.salary_floor IS NULL THEN 0.5
            WHEN e.salary_min IS NULL AND e.salary_max IS NULL THEN 0.0
            WHEN GREATEST(COALESCE(e.salary_min, -1e18), COALESCE(e.salary_max, -1e18)) >= e.salary_floor THEN 1.0
            WHEN e.salary_floor <= 0 THEN 0.0
            ELSE GREATEST(0.0, LEAST(1.0, GREATEST(COALESCE(e.salary_min, -1e18), COALESCE(e.salary_max, -1e18)) / e.salary_floor))
        END AS s_compensation
    FROM eligibility e
    CROSS JOIN weights w
),
ranked AS (
    SELECT
        s.*,
        GREATEST(0.0, LEAST(1.0,
            s.s_skills * s.w_skills
            + s.s_title * s.w_title
            + s.s_location_work_mode * s.w_location_work_mode
            + s.s_seniority * s.w_seniority
            + s.s_recency * s.w_recency
            + s.s_compensation * s.w_compensation
        )) AS total_score
    FROM scored s
)
SELECT
    r.job_id,
    CASE WHEN CARDINALITY(r.exclude_reasons) = 0 THEN ROUND(r.total_score::NUMERIC, 6)::DOUBLE PRECISION ELSE 0.0 END AS score,
    (
        SELECT COALESCE(JSONB_AGG(item.obj ORDER BY item.contribution DESC), '[]'::JSONB)
        FROM (
            SELECT contribution, JSONB_BUILD_OBJECT(
                'code', code,
                'value', ROUND(value::NUMERIC, 6)::DOUBLE PRECISION,
                'weight', ROUND(weight::NUMERIC, 6)::DOUBLE PRECISION,
                'contribution', ROUND(contribution::NUMERIC, 6)::DOUBLE PRECISION
            ) AS obj
            FROM (
                VALUES
                    ('SIGNAL_SKILLS', r.s_skills, r.w_skills, r.s_skills * r.w_skills),
                    ('SIGNAL_TITLE', r.s_title, r.w_title, r.s_title * r.w_title),
                    ('SIGNAL_LOCATION_WORK_MODE', r.s_location_work_mode, r.w_location_work_mode, r.s_location_work_mode * r.w_location_work_mode),
                    ('SIGNAL_SENIORITY', r.s_seniority, r.w_seniority, r.s_seniority * r.w_seniority),
                    ('SIGNAL_RECENCY', r.s_recency, r.w_recency, r.s_recency * r.w_recency),
                    ('SIGNAL_COMPENSATION', r.s_compensation, r.w_compensation, r.s_compensation * r.w_compensation)
            ) AS raw(code, value, weight, contribution)
            ORDER BY contribution DESC
            LIMIT r.reason_limit
        ) item
    ) AS match_reasons,
    (
        SELECT COALESCE(ARRAY_AGG(skill ORDER BY skill), ARRAY[]::TEXT[])
        FROM UNNEST(r.candidate_skills) AS skill
        WHERE NOT skill = ANY(r.skill_tokens)
    ) AS required_missing_skills,
    r.apply_url,
    r.posted_at,
    r.source,
    COALESCE(r.quality_flags, ARRAY[]::TEXT[]) AS quality_flags,
    CARDINALITY(r.exclude_reasons) > 0 AS excluded,
    r.exclude_reasons
FROM ranked r
CROSS JOIN candidate_raw c
WHERE include_excluded OR CARDINALITY(r.exclude_reasons) = 0
ORDER BY (CARDINALITY(r.exclude_reasons) > 0) ASC, score DESC, r.posted_at DESC NULLS LAST, r.job_id ASC
LIMIT c.top_k;
$$;
""".strip()
