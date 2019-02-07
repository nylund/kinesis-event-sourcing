CREATE TABLE `input_records` (
  `id` binary(16) not null,
  `ts` datetime not null,
  UNIQUE KEY id_unique (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;