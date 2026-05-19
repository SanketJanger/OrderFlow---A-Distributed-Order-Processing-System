-- Products table
CREATE TABLE products (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    stock INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Orders table
CREATE TABLE orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    total_amount DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Order items table
CREATE TABLE order_items (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID REFERENCES orders(id),
    product_id UUID REFERENCES products(id),
    quantity INTEGER NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

-- Jobs table
CREATE TABLE jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id UUID REFERENCES orders(id),
    job_type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    priority VARCHAR(20) NOT NULL DEFAULT 'medium',
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3,
    error_message TEXT,
    result JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Dead letter queue table
CREATE TABLE dead_letter_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES jobs(id),
    order_id UUID REFERENCES orders(id),
    job_type VARCHAR(50) NOT NULL,
    error_message TEXT,
    failed_at TIMESTAMP DEFAULT NOW(),
    payload JSONB
);

-- Seed products
INSERT INTO products (name, price, stock) VALUES
    ('MacBook Pro 14"', 1999.99, 10),
    ('iPhone 15 Pro', 999.99, 25),
    ('AirPods Pro', 249.99, 50),
    ('iPad Air', 599.99, 15),
    ('Apple Watch Series 9', 399.99, 20);

-- Indexes
CREATE INDEX idx_orders_status ON orders(status);
CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_order_id ON jobs(order_id);
CREATE INDEX idx_jobs_priority ON jobs(priority);
