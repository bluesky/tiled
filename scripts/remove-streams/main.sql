-- Tree Migration Script: Remove 'streams' namespaced nodes from BlueskyRuns

-- Move children of every node named 'streams' up to the parent of that 'streams' node,
-- update closure table accordingly, and then delete the 'streams' nodes.
-- Run on PostgreSQL. Test on a copy first.

BEGIN;

-- =============================================================================
-- (1) Identify the 'streams' nodes that belong to BlueskyRun parents and whose children are all BlueskyEventStream
-- =============================================================================

WITH candidate_streams AS (
    SELECT
        s.id          AS stream_id,
        s.parent      AS parent_id,
        p.id          AS parent_node_id,
        p.specs       AS parent_specs
    FROM nodes s
    JOIN nodes p ON s.parent = p.id
    WHERE s.key = 'streams'
      AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements(p.specs) AS spec
          WHERE spec->>'name' = 'BlueskyRun'
      )
),
streams_with_good_children AS (
    SELECT
        cs.stream_id AS stream_id,
        cs.parent_id AS parent_id
    FROM candidate_streams cs
    WHERE NOT EXISTS (
        SELECT 1
        FROM nodes c
        WHERE c.parent = cs.stream_id
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(c.specs) AS spec
              WHERE spec->>'name' = 'BlueskyEventStream'
          )
    )
)
-- From here on, use only these filtered 'streams' nodes.
-- Store them into a temp table for re-use in multiple statements.
SELECT * INTO TEMP TABLE selected_streams FROM streams_with_good_children;

-- Debugging check: see which streams we’re about to process (Optional; comment out for production)
DO $$
BEGIN
    RAISE NOTICE 'Number of selected ''streams'' nodes to be removed: %',
        (SELECT COUNT(*) FROM selected_streams);
END$$;


-- ============================================================
-- (1b) Rename the selected 'streams' nodes to avoid transient key conflicts.
-- ============================================================

UPDATE nodes
SET key = '_streams_to_be_deleted'
WHERE id IN (SELECT stream_id FROM selected_streams);


-- =============================================================================
-- (2) Conflict detection — prevent name collisions under new parents.
-- =============================================================================

DO $$
DECLARE
    conflict RECORD;
BEGIN
    FOR conflict IN
        SELECT c.id AS child_id, c.key AS child_key, s.parent_id
        FROM selected_streams s
        JOIN nodes c ON c.parent = s.stream_id
        JOIN nodes existing
          ON existing.parent IS NOT DISTINCT FROM s.parent_id
         AND existing.key = c.key
         AND existing.id <> c.id
    LOOP
        RAISE EXCEPTION
            'Aborting: moving child "%" (id=%) would conflict with existing node of same key under parent id=%',
            conflict.child_key, conflict.child_id, conflict.parent_id;
    END LOOP;
    RAISE NOTICE 'No name conflicts detected. Proceeding with migration.';
END$$;


-- ====================================================================
-- 3) Reparent children: set each child.parent = stream.parent
-- ====================================================================

DO $$
DECLARE moved_count integer;
BEGIN
    UPDATE nodes n
    SET parent = s.parent_id
    FROM selected_streams s
    WHERE n.parent = s.stream_id;

    GET DIAGNOSTICS moved_count = ROW_COUNT;
    RAISE NOTICE 'Reparented % node(s) (children of selected streams).', moved_count;
END$$;

-- ====================================================================
-- 4) Build the set of all descendants of the streams nodes (including the stream node itself).
--    We'll use this set to adjust closure rows.
-- ====================================================================

CREATE TEMP TABLE descendants_to_move AS
SELECT DISTINCT c.descendant
FROM selected_streams s
JOIN nodes_closure c ON c.ancestor = s.stream_id;

DO $$
DECLARE dcount integer;
BEGIN
    SELECT COUNT(*) INTO dcount FROM descendants_to_move;
    RAISE NOTICE 'Total distinct descendant node(s) to move = %.', dcount;
END$$;

-- ====================================================================
-- 5) Adjust the closure table: for any (ancestor, descendant) where
--      descendant IN the set of descendants AND  ancestor NOT IN the
--      set of descendants, reduce depth by 1.
-- ====================================================================
DO $$
DECLARE adjust_count integer;
BEGIN
    UPDATE nodes_closure nc
    SET depth = nc.depth - 1
    FROM descendants_to_move d
    WHERE nc.descendant = d.descendant
      AND nc.ancestor NOT IN (SELECT descendant FROM descendants_to_move)
      AND nc.depth > 0;  -- defensive: only decrement positive depths

    GET DIAGNOSTICS adjust_count = ROW_COUNT;
    RAISE NOTICE 'Adjusted depth (decremented by 1) for % closure rows.', adjust_count;
END$$;

-- ====================================================================
-- 6) Delete the selected stream nodes. Their nodes_closure rows that
--    reference them will be removed by ON DELETE CASCADE on the FK.
-- ====================================================================
DO $$
DECLARE del_count integer;
BEGIN
    DELETE FROM nodes
    WHERE id IN (SELECT stream_id FROM selected_streams);

    GET DIAGNOSTICS del_count = ROW_COUNT;
    RAISE NOTICE 'Deleted % stream node(s).', del_count;
END$$;

COMMIT;