CREATE TABLE IF NOT EXISTS product_staging_table(
    id SERIAL PRIMARY KEY,
    file_name VARCHAR(20) NOT NULL,
    file_location VARCHAR(40) NOT NULL,
    created_date TIMESTAMP DEFAULT NOW(),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status CHAR(1) DEFAULT 'I'
);

INSERT INTO product_staging_table(file_name, file_location, status)
VALUES ('sales.csv', 'sales.csv', 'A'),
        ('sales.csv', 'sales.csv', 'I'),
        ('sales.csv', 'sales.csv', 'A'),
        ('sales.csv', 'sales.csv', 'A'),
        ('abc.csv', 'abc.csv', 'A'),
        ('abc.csv', 'abc.csv', 'I'),
        ('abc.csv', 'abc.csv', 'I')
