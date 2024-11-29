-- Tạo bảng products
CREATE TABLE IF NOT EXISTS products (
    id BIGINT PRIMARY KEY,                   -- ID sản phẩm
    sku TEXT,                                -- SKU sản phẩm
    name TEXT NOT NULL,                      -- Tên sản phẩm
    price NUMERIC NOT NULL,                  -- Giá sản phẩm
    list_price NUMERIC,                      -- Giá gốc (trước giảm giá)
    discount NUMERIC,                        -- Giá trị giảm giá
    discount_rate NUMERIC,                   -- Tỷ lệ giảm giá (%)
    rating_average NUMERIC,                  -- Điểm đánh giá trung bình
    review_count INT,                        -- Số lượng đánh giá
    order_count INT,                         -- Số lượng đơn hàng
    favourite_count INT,                     -- Số lượng yêu thích
    thumbnail_url TEXT,                      -- URL ảnh đại diện
    quantity_sold INT,                       -- Số lượng đã bán
    original_price NUMERIC,                  -- Giá gốc (không khuyến mãi)
    seller_id BIGINT,                        -- ID nhà bán
    seller TEXT,                             -- Tên nhà bán
    seller_product_id BIGINT,               -- ID sản phẩm từ nhà bán
    brand_name TEXT,
    category_l1_name TEXT,        -- Tên danh mục cấp 1
    category_l2_name TEXT,                 -- Tên danh mục cấp 2
    category_l3_name TEXT,                      -- Tên danh mục cấp 3
);
