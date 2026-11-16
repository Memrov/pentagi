-- +goose Up
-- +goose StatementBegin
CREATE TYPE FLOW_ENGINE AS ENUM ('legacy','adk');

ALTER TABLE flows ADD COLUMN engine FLOW_ENGINE NOT NULL DEFAULT 'legacy';
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
ALTER TABLE flows DROP COLUMN engine;

DROP TYPE FLOW_ENGINE;
-- +goose StatementEnd
