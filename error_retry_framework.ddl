CREATE TABLE public.error_retry_framework (
	id serial NOT NULL,
	process_group varchar(255) NOT NULL,
	processor varchar(255) NOT NULL,
	route varchar(55) NOT NULL,
	attempt int4 NOT NULL
);
