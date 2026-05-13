

INSERT INTO platform (ea_namespace, platform_name) VALUES
('xbox', 'Xbox'),
('xbl_sub', 'Xbox'),
('ps4', 'PlayStation 4'),
('ps3', 'PlayStation 3'),
('cem_ea_id', 'PC')
ON CONFLICT (ea_namespace) DO NOTHING;