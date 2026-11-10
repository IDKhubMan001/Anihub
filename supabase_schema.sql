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
