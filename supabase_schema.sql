-- =====================================================================
-- AniHub · Схема внешней базы данных PostgreSQL (Supabase / Neon.tech)
-- Примечание: server.js выполняет эту миграцию АВТОМАТИЧЕСКИ при старте,
-- как только задан DATABASE_URL. Запускать вручную не обязательно.
-- =====================================================================

-- 1. Таблица аккаунтов (users)
-- Аватарки хранятся в облаке (Supabase Storage / Cloudflare R2), здесь только короткий URL.
CREATE TABLE IF NOT EXISTS users (
  nick_lower TEXT PRIMARY KEY,
  nick       TEXT NOT NULL,
  email      TEXT NOT NULL DEFAULT '',
  salt       TEXT NOT NULL,
  pass_hash  TEXT NOT NULL,
  avatar_url TEXT NOT NULL DEFAULT '',
  tokens     JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_email_lower
  ON users (LOWER(email))
  WHERE email <> '';

-- 2. Таблица комментариев к тайтлам (comments)
CREATE TABLE IF NOT EXISTS comments (
  id          TEXT PRIMARY KEY,
  title_id    TEXT NOT NULL,
  uid         TEXT NOT NULL,
  author_name TEXT NOT NULL,
  body        TEXT NOT NULL,
  parent_id   TEXT,
  parent_name TEXT NOT NULL DEFAULT '',
  likes       JSONB NOT NULL DEFAULT '[]'::jsonb,
  dislikes    JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_comments_title_created
  ON comments (title_id, created_at ASC);

-- 3. Таблица истории просмотров (history)
-- На уровне СУБД триггер trg_history_keep_latest_ep автоматически удаляет
-- старые серии одного и того же аниме (title_id) у пользователя,
-- оставляя в базе ТОЛЬКО актуальную серию!
CREATE TABLE IF NOT EXISTS history (
  id          BIGSERIAL PRIMARY KEY,
  nick_lower  TEXT NOT NULL,
  title_id    TEXT NOT NULL,
  ep          TEXT NOT NULL,
  ep_num      INTEGER NOT NULL DEFAULT 1,
  title       TEXT NOT NULL DEFAULT '',
  poster_url  TEXT NOT NULL DEFAULT '',
  ep_title    TEXT NOT NULL DEFAULT '',
  pos         DOUBLE PRECISION NOT NULL DEFAULT 0,
  dur         DOUBLE PRECISION NOT NULL DEFAULT 0,
  done        BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at  BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_history_user_updated
  ON history (nick_lower, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_history_user_title
  ON history (nick_lower, title_id);

CREATE OR REPLACE FUNCTION fn_history_keep_latest_ep()
RETURNS TRIGGER AS $$
BEGIN
  DELETE FROM history
  WHERE nick_lower = NEW.nick_lower
    AND title_id = NEW.title_id;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_history_keep_latest_ep ON history;
CREATE TRIGGER trg_history_keep_latest_ep
BEFORE INSERT ON history
FOR EACH ROW
EXECUTE FUNCTION fn_history_keep_latest_ep();

-- 4. Публичные настройки и списки профиля (избранное / смотрю / в планах / брошено)
CREATE TABLE IF NOT EXISTS user_profiles (
  nick_lower TEXT PRIMARY KEY,
  name       TEXT NOT NULL,
  lists      JSONB NOT NULL DEFAULT '{}'::jsonb,
  pub        BOOLEAN NOT NULL DEFAULT TRUE,
  updated_at BIGINT NOT NULL
);

-- 5. Друзья (двустороннее подтверждение) и личные сообщения (ЛС)
CREATE TABLE IF NOT EXISTS friends (
  pair_key   TEXT PRIMARY KEY,
  user_a     TEXT NOT NULL,
  user_b     TEXT NOT NULL,
  nick_a     TEXT NOT NULL,
  nick_b     TEXT NOT NULL,
  status     TEXT NOT NULL,
  requester  TEXT NOT NULL,
  created_at BIGINT NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_friends_user_a ON friends (user_a);
CREATE INDEX IF NOT EXISTS idx_friends_user_b ON friends (user_b);

CREATE TABLE IF NOT EXISTS dms (
  pair_key   TEXT PRIMARY KEY,
  rev        INTEGER NOT NULL DEFAULT 0,
  msgs       JSONB NOT NULL DEFAULT '[]'::jsonb,
  read_at    JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at BIGINT NOT NULL
);

-- 6. Таймкоды опенингов/эндингов, анти-спам и модерация (без локальных файлов)
CREATE TABLE IF NOT EXISTS ovr_timings (
  ep_key     TEXT PRIMARY KEY,
  intro      JSONB,
  outro      JSONB,
  author     TEXT NOT NULL DEFAULT '',
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS penalties (
  actor_key  TEXT PRIMARY KEY,
  data       JSONB NOT NULL,
  updated_at BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS moderation_kv (
  k          TEXT PRIMARY KEY,
  v          JSONB NOT NULL,
  updated_at BIGINT NOT NULL
);

ALTER TABLE users ENABLE ROW LEVEL SECURITY;
ALTER TABLE comments ENABLE ROW LEVEL SECURITY;
ALTER TABLE history ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE friends ENABLE ROW LEVEL SECURITY;
ALTER TABLE dms ENABLE ROW LEVEL SECURITY;
ALTER TABLE ovr_timings ENABLE ROW LEVEL SECURITY;
ALTER TABLE penalties ENABLE ROW LEVEL SECURITY;
ALTER TABLE moderation_kv ENABLE ROW LEVEL SECURITY;

-- 7. Защищённый серверный RPC для работы server.js через SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY
-- Доступ открыт СТРОГО роли service_role (публичные/анонимные ключи получают 401 permission denied).
CREATE OR REPLACE FUNCTION public.anihub_srv_query(q text, p jsonb DEFAULT '[]'::jsonb)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  sql_exec text := q;
  trimmed  text := btrim(q);
  n        int  := COALESCE(jsonb_array_length(p), 0);
  i        int;
  elem     jsonb;
  t        text;
  rep      text;
  res      jsonb;
BEGIN
  IF COALESCE(current_setting('request.jwt.claim.role', true), '') <> 'service_role'
     AND current_user NOT IN ('postgres', 'supabase_admin') THEN
    RAISE EXCEPTION 'permission denied for anihub_srv_query';
  END IF;

  FOR i IN REVERSE n..1 LOOP
    elem := p -> (i - 1);
    t := jsonb_typeof(elem);
    IF t IS NULL OR t = 'null' THEN
      rep := 'NULL';
    ELSIF t = 'boolean' THEN
      rep := CASE WHEN (elem)::boolean THEN 'TRUE' ELSE 'FALSE' END;
    ELSIF t = 'number' THEN
      rep := elem #>> '{}';
    ELSIF t = 'string' THEN
      rep := quote_literal(elem #>> '{}');
    ELSE
      rep := quote_literal(elem::text);
    END IF;
    sql_exec := regexp_replace(sql_exec, '\$' || i::text || '(?![0-9])', replace(rep, '\', '\\'), 'g');
  END LOOP;

  sql_exec := regexp_replace(sql_exec, ';\s*$', '');

  IF trimmed ~* '^(select|with)\M' OR trimmed ~* '\mreturning\M' THEN
    EXECUTE 'WITH _r AS (' || sql_exec || ') SELECT COALESCE(jsonb_agg(row_to_json(_r)), ''[]''::jsonb) FROM _r' INTO res;
    RETURN COALESCE(res, '[]'::jsonb);
  ELSE
    EXECUTE sql_exec;
    RETURN '[]'::jsonb;
  END IF;
END;
$$;

REVOKE ALL ON FUNCTION public.anihub_srv_query(text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION public.anihub_srv_query(text, jsonb) FROM anon;
REVOKE ALL ON FUNCTION public.anihub_srv_query(text, jsonb) FROM authenticated;
GRANT EXECUTE ON FUNCTION public.anihub_srv_query(text, jsonb) TO service_role;

-- 8. Защищённый публичный RPC для автономной работы комментариев напрямую в Supabase
-- без участия Render (чтение, отправка с анти-спамом ссылок, ответы, лайки/дизлайки, удаление своих).
CREATE OR REPLACE FUNCTION public.anihub_public_comments(
  p_tid text,
  p_act text DEFAULT 'list',
  p_payload jsonb DEFAULT '{}'::jsonb
)
RETURNS jsonb
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
  v_tid text;
  v_now bigint;
  v_list jsonb;
  v_avas jsonb;
  v_rev bigint;
  v_uid text;
  v_token text;
  v_tok_nick text;
  v_name text;
  v_text text;
  v_parent_in text;
  v_parent_id text;
  v_parent_name text;
  v_pen jsonb;
  v_id text;
  v_cid text;
  v_v int;
  v_can_del boolean;
BEGIN
  v_tid := left(regexp_replace(coalesce(p_tid, ''), '[^a-zA-Z0-9_-]', '', 'g'), 40);
  v_now := (extract(epoch from clock_timestamp()) * 1000)::bigint;
  IF v_tid = '' THEN
    RETURN jsonb_build_object('ok', false, 'status', 400, 'error', 'плохой id тайтла', 'serverNow', v_now);
  END IF;

  IF p_act = 'list' THEN
    SELECT
      coalesce(jsonb_agg(
        jsonb_build_object(
          'id', c.id,
          'uid', c.uid,
          'name', c.author_name,
          'text', c.body,
          'at', c.created_at,
          'parent', c.parent_id,
          'parentName', c.parent_name,
          'likes', c.likes,
          'dislikes', c.dislikes
        ) ORDER BY c.created_at ASC
      ), '[]'::jsonb),
      coalesce(max(c.created_at), 0)
        + count(*)::bigint * 7
        + coalesce(sum(jsonb_array_length(c.likes) * 13 + jsonb_array_length(c.dislikes) * 17), 0)::bigint
    INTO v_list, v_rev
    FROM (
      SELECT id, uid, author_name, body, parent_id, parent_name, likes, dislikes, created_at
      FROM public.comments
      WHERE title_id = v_tid
      ORDER BY created_at ASC
      LIMIT 400
    ) c;

    SELECT coalesce(jsonb_object_agg(u.nick_lower, u.avatar_url), '{}'::jsonb)
    INTO v_avas
    FROM public.users u
    WHERE u.avatar_url <> ''
      AND u.nick_lower IN (
        SELECT DISTINCT lower(author_name) FROM public.comments WHERE title_id = v_tid
      );

    RETURN jsonb_build_object('ok', true, 'rev', v_rev, 'list', v_list, 'avatars', v_avas, 'serverNow', v_now);
  END IF;

  v_uid := left(regexp_replace(coalesce(p_payload->>'uid', ''), '[^a-zA-Z0-9_.:-]', '', 'g'), 40);
  IF v_uid = '' THEN v_uid := '?'; END IF;
  v_token := left(coalesce(p_payload->>'token', ''), 120);
  v_tok_nick := NULL;
  IF v_token <> '' THEN
    SELECT u.nick INTO v_tok_nick
    FROM public.users u
    WHERE u.tokens ? v_token
    LIMIT 1;
  END IF;

  IF p_act = 'add' THEN
    v_text := btrim(left(regexp_replace(coalesce(p_payload->>'text', ''), '[\x00-\x08\x0B\x0C\x0E-\x1F]', '', 'g'), 1200));
    IF v_text = '' THEN
      RETURN jsonb_build_object('ok', false, 'status', 400, 'error', 'пустой комментарий', 'serverNow', v_now);
    END IF;

    v_name := coalesce(v_tok_nick, btrim(left(regexp_replace(coalesce(p_payload->>'name', ''), '[\x00-\x1F<>]', '', 'g'), 24)));
    IF v_name IS NULL OR v_name = '' THEN v_name := 'Гость'; END IF;

    SELECT p.data INTO v_pen
    FROM public.penalties p
    WHERE p.actor_key IN ('acct:' || lower(v_name), 'uid:' || v_uid)
    ORDER BY coalesce((p.data->>'permBan')::boolean, false) DESC,
             coalesce((p.data->>'tempBanUntil')::bigint, 0) DESC,
             coalesce((p.data->>'mutedUntil')::bigint, 0) DESC
    LIMIT 1;

    IF v_pen IS NOT NULL THEN
      IF coalesce((v_pen->>'permBan')::boolean, false) THEN
        RETURN jsonb_build_object('ok', false, 'status', 403, 'code', 'PERM_BAN', 'error', 'Чат и комментарии заблокированы навсегда', 'serverNow', v_now);
      END IF;
      IF coalesce((v_pen->>'tempBanUntil')::bigint, 0) > v_now THEN
        RETURN jsonb_build_object('ok', false, 'status', 403, 'code', 'TEMP_BAN', 'error', 'Чат и комментарии временно заблокированы', 'serverNow', v_now);
      END IF;
      IF coalesce((v_pen->>'mutedUntil')::bigint, 0) > v_now THEN
        RETURN jsonb_build_object('ok', false, 'status', 403, 'code', 'MUTED', 'error', 'Вы в муте ещё ' || ceil((coalesce((v_pen->>'mutedUntil')::bigint, 0) - v_now) / 1000.0)::int || ' сек', 'serverNow', v_now);
      END IF;
    END IF;

    IF v_text ~* '(https?://|www\.|t\.me/|discord\.gg/|vk\.cc/|bit\.ly/|[a-z0-9-]+\.(ru|com|net|org|io|me|gg|xyz|top|site|club|online|info|shop|fun|pw|su|tv|cc)(/|$|\s))' THEN
      RETURN jsonb_build_object('ok', false, 'status', 403, 'deleted', true, 'code', 'LINK_MUTE', 'error', 'Ссылки запрещены: сообщение удалено', 'serverNow', v_now);
    END IF;

    IF v_uid <> '?' AND EXISTS (
      SELECT 1 FROM public.comments c2 WHERE c2.uid = v_uid AND c2.created_at > v_now - 1500
    ) THEN
      RETURN jsonb_build_object('ok', false, 'status', 429, 'error', 'Подождите пару секунд перед следующим комментарием', 'serverNow', v_now);
    END IF;

    v_parent_in := left(coalesce(p_payload->>'parent', ''), 60);
    v_parent_id := NULL;
    v_parent_name := '';
    IF v_parent_in <> '' THEN
      SELECT coalesce(cp.parent_id, cp.id), cp.author_name
      INTO v_parent_id, v_parent_name
      FROM public.comments cp
      WHERE cp.id = v_parent_in AND cp.title_id = v_tid
      LIMIT 1;
      IF v_parent_name IS NULL THEN v_parent_name := ''; END IF;
    END IF;

    v_id := 'c' || to_hex(v_now) || substr(md5(random()::text || clock_timestamp()::text), 1, 6);
    INSERT INTO public.comments (id, title_id, uid, author_name, body, parent_id, parent_name, likes, dislikes, created_at)
    VALUES (v_id, v_tid, v_uid, v_name, v_text, v_parent_id, v_parent_name, '[]'::jsonb, '[]'::jsonb, v_now);

    RETURN jsonb_build_object('ok', true, 'id', v_id, 'serverNow', v_now);
  END IF;

  IF p_act = 'vote' THEN
    v_cid := left(coalesce(p_payload->>'cid', ''), 60);
    v_v := coalesce((p_payload->>'v')::int, 0);
    IF v_cid = '' OR v_uid = '?' THEN
      RETURN jsonb_build_object('ok', false, 'status', 400, 'error', 'плохой голос', 'serverNow', v_now);
    END IF;

    UPDATE public.comments c
    SET
      likes = CASE
        WHEN v_v = 1 THEN (
          SELECT coalesce(jsonb_agg(x), '[]'::jsonb)
          FROM (
            SELECT elem AS x FROM jsonb_array_elements(c.likes) elem WHERE elem #>> '{}' <> v_uid
            UNION ALL
            SELECT to_jsonb(v_uid)
          ) s1
        )
        ELSE (
          SELECT coalesce(jsonb_agg(elem), '[]'::jsonb)
          FROM jsonb_array_elements(c.likes) elem WHERE elem #>> '{}' <> v_uid
        )
      END,
      dislikes = CASE
        WHEN v_v = -1 THEN (
          SELECT coalesce(jsonb_agg(x), '[]'::jsonb)
          FROM (
            SELECT elem AS x FROM jsonb_array_elements(c.dislikes) elem WHERE elem #>> '{}' <> v_uid
            UNION ALL
            SELECT to_jsonb(v_uid)
          ) s2
        )
        ELSE (
          SELECT coalesce(jsonb_agg(elem), '[]'::jsonb)
          FROM jsonb_array_elements(c.dislikes) elem WHERE elem #>> '{}' <> v_uid
        )
      END
    WHERE c.id = v_cid AND c.title_id = v_tid;

    RETURN jsonb_build_object('ok', true, 'id', v_cid, 'serverNow', v_now);
  END IF;

  IF p_act = 'del' THEN
    v_cid := left(coalesce(p_payload->>'cid', ''), 60);
    IF v_cid = '' THEN
      RETURN jsonb_build_object('ok', false, 'status', 400, 'error', 'нет cid', 'serverNow', v_now);
    END IF;

    SELECT EXISTS (
      SELECT 1 FROM public.comments cd
      WHERE cd.id = v_cid AND cd.title_id = v_tid
        AND (
          (v_uid <> '?' AND cd.uid = v_uid)
          OR (v_tok_nick IS NOT NULL AND lower(cd.author_name) = lower(v_tok_nick))
        )
    ) INTO v_can_del;

    IF NOT v_can_del THEN
      RETURN jsonb_build_object('ok', false, 'status', 403, 'error', 'Можно удалять только свои комментарии', 'serverNow', v_now);
    END IF;

    DELETE FROM public.comments
    WHERE title_id = v_tid AND (id = v_cid OR parent_id = v_cid);

    RETURN jsonb_build_object('ok', true, 'deleted', v_cid, 'serverNow', v_now);
  END IF;

  RETURN jsonb_build_object('ok', false, 'status', 400, 'error', 'неизвестное действие', 'serverNow', v_now);
END;
$$;

REVOKE ALL ON FUNCTION public.anihub_public_comments(text, text, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.anihub_public_comments(text, text, jsonb) TO anon, authenticated, service_role;
NOTIFY pgrst, 'reload schema';

