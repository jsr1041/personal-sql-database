-- ============================================================
-- 006_route.sql
-- Route definitions
-- ============================================================

CREATE TABLE public.route (
	route_id serial4 NOT NULL,
	name varchar(255) NOT NULL,
	description text NULL,
	distance_miles numeric(6, 2) NULL,
	elevation_gain_feet numeric(7, 1) NULL,
	surface_type varchar(50) NULL,
	start_lat numeric(9, 6) NOT NULL,
	start_long numeric(9, 6) NOT NULL,
	end_lat numeric(9, 6) NULL,
	end_long numeric(9, 6) NULL,
	gps_path jsonb NULL,
	notes text NULL,
	source_system varchar(100) NULL,
	source_record_id varchar(255) NULL,
	created_at timestamptz DEFAULT now() NOT NULL,
	updated_at timestamptz DEFAULT now() NOT NULL,
	is_deleted bool DEFAULT false NOT NULL,
	CONSTRAINT route_pkey PRIMARY KEY (route_id)
);

CREATE INDEX idx_route_start ON public.route USING btree (start_lat, start_long);
