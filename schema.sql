-- Apply on the VM after the database exists:
--   psql "$DATABASE_URL" -f schema.sql
-- The loader checks rows before this replacement. Foreign keys are deferred
-- so one tournament is deleted and inserted inside a single transaction.

create schema if not exists chgk;

create table if not exists chgk.tournaments (
    tournament_id integer primary key,
    tournament_name text,
    tournament_long_name text,
    date_start date,
    date_end date,
    last_edit_date date,
    type_id integer,
    type_name text,
    season_id integer,
    difficulty_forecast double precision,
    true_dl double precision,
    question_qty jsonb,
    teams_count integer,
    rating_systems text,
    has_chgk_rating boolean,
    is_synch boolean,
    editors_ids jsonb,
    orgcommittee_ids jsonb,
    game_jury jsonb,
    appeal_jury jsonb,
    narrator jsonb,
    inserted_at timestamptz not null
);

create table if not exists chgk.tournaments_with_teams (
    tournament_id integer not null,
    team_id integer not null,
    team_name text,
    current_team_name text,
    position double precision,
    predicted_position double precision,
    team_played_date date,
    venue_id integer,
    venue_name text,
    town_name text,
    in_rating boolean,
    rg double precision,
    r double precision,
    rb double precision,
    rt double precision,
    b double precision,
    d double precision,
    d1 double precision,
    d2 double precision,
    bp double precision,
    potential double precision,
    c_star double precision,
    player_ids jsonb,
    flags jsonb,
    inserted_at timestamptz not null,
    primary key (tournament_id, team_id),
    foreign key (tournament_id)
        references chgk.tournaments (tournament_id)
        deferrable initially deferred
);

create table if not exists chgk.tournaments_with_players (
    tournament_id integer not null,
    team_id integer not null,
    player_id integer not null,
    player_name text,
    player_surname text,
    player_patronymic text,
    flag text,
    player_rating double precision,
    inserted_at timestamptz not null,
    primary key (tournament_id, team_id, player_id),
    foreign key (tournament_id, team_id)
        references chgk.tournaments_with_teams (tournament_id, team_id)
        deferrable initially deferred
);

-- tiebreak_round 0 is the main packet. 1, 2, ... are shootout rounds.
-- Question numbers restart inside each shootout round.
create table if not exists chgk.tournaments_with_questions (
    tournament_id integer not null,
    team_id integer not null,
    tiebreak_round integer not null,
    question_number integer not null,
    mask_char text,
    taken smallint,
    difficulty double precision,
    question_potential double precision,
    inserted_at timestamptz not null,
    primary key (tournament_id, team_id, tiebreak_round, question_number),
    foreign key (tournament_id, team_id)
        references chgk.tournaments_with_teams (tournament_id, team_id)
        deferrable initially deferred
);

create table if not exists chgk.tournament_load (
    tournament_id integer primary key,
    last_edit_date date,
    status text not null,
    error text,
    inserted_at timestamptz not null,
    foreign key (tournament_id)
        references chgk.tournaments (tournament_id)
        deferrable initially deferred
);
