CREATE TABLE vendas (
  venda_id SERIAL PRIMARY KEY,
  data_venda DATE NOT NULL,
  valor DECIMAL(10, 2) NOT NULL,
  quantidade INT NOT NULL,
  cliente_id INT NOT NULL,
  categoria VARCHAR(255) NOT NULL
);

CREATE TABLE vendas_calculado (
  venda_id SERIAL PRIMARY KEY,
  data_venda DATE NOT NULL,
  valor DECIMAL(10, 2) NOT NULL,
  quantidade INT NOT NULL,
  cliente_id INT NOT NULL,
  categoria VARCHAR(255) NOT null,
  total_vendas DECIMAL(10, 2) NOT NULL
);


INSERT INTO vendas (data_venda, valor, quantidade, cliente_id, categoria) VALUES 
('2024-01-01', 66.1, 10, 30, 'Alimentos'),
('2024-01-01', 232.65, 4, 15, 'Brinquedos'),
('2024-01-01', 346.06, 8, 26, 'Eletrônicos'),
('2024-01-01', 471.69, 3, 20, 'Alimentos'),
('2024-01-01', 302.13, 3, 30, 'Brinquedos'),
('2024-01-01', 357.33, 2, 28, 'Alimentos'),
('2024-01-01', 262.89, 7, 20, 'Roupas'),
('2024-01-01', 339.8, 6, 15, 'Roupas'),
('2024-01-01', 334.23, 9, 15, 'Brinquedos'),
('2024-01-01', 91.64, 10, 8, 'Eletrônicos'),
('2024-01-02', 414.65, 9, 5, 'Roupas'),
('2024-01-02', 203.23, 3, 15, 'Eletrônicos'),
('2024-01-02', 263.03, 10, 7, 'Eletrônicos'),
('2024-01-02', 328.95, 1, 8, 'Roupas'),
('2024-01-02', 302.71, 5, 27, 'Alimentos'),
('2024-01-02', 341.13, 9, 7, 'Brinquedos'),
('2024-01-02', 132.47, 8, 12, 'Brinquedos'),
('2024-01-02', 423.71, 5, 7, 'Roupas'),
('2024-01-02', 86.72, 3, 8, 'Brinquedos'),
('2024-01-02', 186.0, 4, 14, 'Livros'),
('2024-01-03', 429.94, 5, 18, 'Alimentos'),
('2024-01-03', 117.96, 2, 2, 'Eletrônicos'),
('2024-01-03', 153.5, 10, 29, 'Roupas'),
('2024-01-03', 498.22, 1, 20, 'Livros'),
('2024-01-03', 487.77, 6, 24, 'Alimentos'),
('2024-01-03', 215.58, 7, 21, 'Brinquedos'),
('2024-01-03', 182.34, 8, 1, 'Livros'),
('2024-01-03', 365.72, 2, 3, 'Eletrônicos'),
('2024-01-03', 145.88, 9, 14, 'Roupas'),
('2024-01-03', 92.53, 1, 22, 'Brinquedos'),
('2024-01-03', 79.75, 5, 4, 'Livros'),
('2024-01-03', 379.87, 4, 19, 'Alimentos'),
('2024-01-03', 104.93, 3, 13, 'Eletrônicos'),
('2024-01-03', 442.36, 7, 11, 'Livros'),
('2024-01-03', 237.68, 6, 10, 'Roupas'),
('2024-01-03', 123.77, 8, 6, 'Brinquedos'),
('2024-01-03', 294.23, 10, 9, 'Alimentos'),
('2024-01-03', 464.57, 4, 25, 'Eletrônicos'),
('2024-01-03', 253.42, 2, 17, 'Livros'),
('2024-01-03', 310.68, 1, 23, 'Roupas'),
('2024-01-04', 439.25, 3, 16, 'Brinquedos'),
('2024-01-04', 490.98, 5, 27, 'Livros'),
('2024-01-04', 88.24, 9, 26, 'Alimentos'),
('2024-01-04', 371.3, 7, 2, 'Eletrônicos'),
('2024-01-04', 456.15, 6, 12, 'Livros'),
('2024-01-04', 139.42, 10, 18, 'Roupas'),
('2024-01-04', 356.78, 8, 4, 'Alimentos'),
('2024-01-04', 280.64, 2, 29, 'Brinquedos'),
('2024-01-04', 235.9, 4, 23, 'Eletrônicos'),
('2024-01-04', 150.0, 1, 50, 'Roupas');



select * from vendas;


INSERT INTO vendas_calculado (data_venda, valor, quantidade, cliente_id, categoria, total_vendas)
SELECT data_venda, valor, quantidade, cliente_id, categoria, (valor * quantidade) AS total_vendas
FROM vendas;


