CREATE TABLE IF NOT EXISTS etl_snapshoot
(
    src_table_name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    trg_index_name character varying(200) COLLATE pg_catalog."default" NOT NULL,
    src_pk_col_name character varying(100) COLLATE pg_catalog."default",
    src_created_timestamp_col_name character varying(100) COLLATE pg_catalog."default",
	src_updated_timestamp_col_name character varying(100) COLLATE pg_catalog."default",
    last_sync_timestamp timestamp without time zone,
    is_active boolean DEFAULT true,
    CONSTRAINT etl_snapshoot_pkey PRIMARY KEY (src_table_name)
);

INSERT INTO public.etl_snapshoot(
	src_table_name, trg_index_name, src_pk_col_name, src_created_timestamp_col_name, src_updated_timestamp_col_name, last_sync_timestamp, is_active)
	VALUES ('ProductLogs', 'ProductLogs', 'Id', 'CreatedAt', 'UpdatedAt', '1900-01-01', true);

INSERT INTO public.etl_snapshoot(
	src_table_name, trg_index_name, src_pk_col_name, src_created_timestamp_col_name, src_updated_timestamp_col_name, last_sync_timestamp, is_active)
	VALUES ('ProductVariantLogs', 'ProductVariantLogs', 'Id', 'CreatedAt', 'UpdatedAt', '1900-01-01', true);


UPDATE public.etl_snapshoot SET last_sync_timestamp='1900-01-01';
