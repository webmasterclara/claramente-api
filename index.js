import express from "express";
import fetch from "node-fetch";
import cors from "cors";

const app = express();

app.use(cors({
  origin: [
    "https://claramente.com",
    "https://www.claramente.com",
  ],
  methods: ["GET", "POST", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
  credentials: false,
}));

app.use(express.json());

const STORE_HASH = process.env.BC_STORE_HASH;
const ACCESS_TOKEN = process.env.BC_ACCESS_TOKEN;

if (!STORE_HASH || !ACCESS_TOKEN) {
  console.error("Missing BC_STORE_HASH or BC_ACCESS_TOKEN in Secrets.");
}

const BASE_URL = `https://api.bigcommerce.com/stores/${STORE_HASH}`;
const PORT = process.env.PORT || 3000;

/* ----------------------------- Core utilities ----------------------------- */

async function bcRequest(method, endpoint, body = null) {
  const options = {
    method,
    headers: {
      "X-Auth-Token": ACCESS_TOKEN,
      "Accept": "application/json",
      "Content-Type": "application/json",
    },
  };

  if (body) {
    options.body = JSON.stringify(body);
  }

  const res = await fetch(`${BASE_URL}${endpoint}`, options);
  const text = await res.text();

  if (!res.ok) {
    throw new Error(`BigCommerce error ${res.status}: ${text}`);
  }

  if (!text) return {};
  return JSON.parse(text);
}

function toInt(value, fallback) {
  const n = parseInt(value, 10);
  return Number.isNaN(n) ? fallback : n;
}

function toFloat(value, fallback = null) {
  const n = parseFloat(value);
  return Number.isNaN(n) ? fallback : n;
}

function buildQuery(params) {
  const search = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== null && value !== "") {
      search.append(key, String(value));
    }
  }

  const query = search.toString();
  return query ? `?${query}` : "";
}

function daysAgoIso(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString();
}

function normalizeString(value) {
  return (value || "").toString().trim().toLowerCase();
}

function safeArray(value) {
  return Array.isArray(value) ? value : [];
}

function uniqueBy(arr, keyFn) {
  const seen = new Set();
  const out = [];
  for (const item of arr) {
    const key = keyFn(item);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(item);
    }
  }
  return out;
}

/* --------------------------- Data fetch helpers --------------------------- */
/*
  GPT / Admin reporting helpers:
  - Safe to modify for analytics, dead stock, category lookups, top sellers, etc.
  - Do NOT mix these helpers with storefront cart quantity logic.
  - Cart-safe route is POST /cart-inventory-check near the bottom of this file.
*/

async function fetchAllProducts({
  limit = 250,
  keyword,
  category_id,
  sku,
  include = "variants",
  maxPages = 50,
} = {}) {
  let page = 1;
  let all = [];
  let pagesFetched = 0;
  const localCategoryId =
    category_id !== undefined && category_id !== null && category_id !== ""
      ? String(category_id)
      : null;

  while (pagesFetched < maxPages) {
    const query = buildQuery({
      page,
      limit,
      keyword,
      sku,
      include,
    });

    const res = await bcRequest("GET", `/v3/catalog/products${query}`);
    let data = safeArray(res.data);

    if (localCategoryId) {
      data = data.filter((product) =>
        safeArray(product.categories).map(String).includes(localCategoryId)
      );
    }

    all = all.concat(data);

    const pagination = res.meta?.pagination;
    if (!pagination || page >= pagination.total_pages || safeArray(res.data).length === 0) {
      break;
    }

    page += 1;
    pagesFetched += 1;
  }

  return all;
}

function productInCategory(productOrRow, categoryId) {
  if (categoryId === undefined || categoryId === null || categoryId === "") return true;
  return safeArray(productOrRow?.categories).map(String).includes(String(categoryId));
}

async function fetchAllCategories({
  limit = 250,
  maxPages = 50,
} = {}) {
  let page = 1;
  let all = [];
  let pagesFetched = 0;

  while (pagesFetched < maxPages) {
    const query = buildQuery({ page, limit });
    const res = await bcRequest("GET", `/v3/catalog/categories${query}`);
    const data = safeArray(res.data);
    all = all.concat(data);

    const pagination = res.meta?.pagination;
    if (!pagination || page >= pagination.total_pages || data.length === 0) {
      break;
    }

    page += 1;
    pagesFetched += 1;
  }

  return all;
}

function normalizePathFromUrl(input) {
  if (!input) return null;
  try {
    const url = new URL(String(input));
    return (url.pathname || "/").replace(/\/+$/, "") || "/";
  } catch {
    const raw = String(input).trim();
    return raw.replace(/https?:\/\/[^/]+/i, "").replace(/\/+$/, "") || "/";
  }
}

async function fetchRecentOrders({
  days = 90,
  limit = 250,
  maxPages = 50,
  status_id,
  customer_id,
} = {}) {
  let page = 1;
  let all = [];
  let pagesFetched = 0;

  while (pagesFetched < maxPages) {
    const query = buildQuery({
      page,
      limit,
      min_date_created: daysAgoIso(days),
      status_id,
      customer_id,
    });

    const res = await bcRequest("GET", `/v2/orders${query}`);
    const data = safeArray(res);
    all = all.concat(data);

    if (data.length < limit) {
      break;
    }

    page += 1;
    pagesFetched += 1;
  }

  return all;
}

async function fetchOrderProducts(orderId) {
  const res = await bcRequest("GET", `/v2/orders/${orderId}/products`);
  return safeArray(res);
}

async function fetchAllCustomers({
  limit = 250,
  maxPages = 50,
} = {}) {
  let page = 1;
  let all = [];
  let pagesFetched = 0;

  while (pagesFetched < maxPages) {
    const endpoint = `/v3/customers?page=${page}&limit=${limit}`;
    const result = await bcRequest("GET", endpoint);
    const data = safeArray(result.data);
    all = all.concat(data);

    const pagination = result.meta?.pagination;
    if (!pagination || page >= pagination.total_pages || data.length === 0) {
      break;
    }

    page += 1;
    pagesFetched += 1;
  }

  return all;
}

/* ------------------------- Inventory / sales helpers ---------------------- */

function flattenInventoryRows(products) {
  const rows = [];

  for (const product of products) {
    rows.push({
      type: "product",
      product_id: product.id,
      variant_id: null,
      product_name: product.name,
      product_sku: product.sku || null,
      variant_sku: null,
      brand_id: product.brand_id ?? null,
      categories: safeArray(product.categories),
      inventory_level: product.inventory_level ?? null,
      inventory_tracking: product.inventory_tracking ?? null,
      price: product.price ?? null,
      cost_price: product.cost_price ?? null,
      bin_picking_number: product.bin_picking_number ?? null,
      is_visible: product.is_visible ?? null,
    });

    for (const variant of safeArray(product.variants)) {
      rows.push({
        type: "variant",
        product_id: product.id,
        variant_id: variant.id,
        product_name: product.name,
        product_sku: product.sku || null,
        variant_sku: variant.sku || null,
        brand_id: product.brand_id ?? null,
        categories: safeArray(product.categories),
        inventory_level: variant.inventory_level ?? null,
        inventory_tracking: variant.inventory_tracking ?? null,
        price: variant.price ?? product.price ?? null,
        cost_price: product.cost_price ?? null,
        bin_picking_number:
          variant.bin_picking_number ?? product.bin_picking_number ?? null,
        is_visible: product.is_visible ?? null,
      });
    }
  }

  return rows;
}

function buildProductLookup(products) {
  const byProductId = new Map();
  const bySku = new Map();

  for (const product of products) {
    byProductId.set(String(product.id), product);

    if (product.sku) {
      bySku.set(normalizeString(product.sku), {
        product_id: product.id,
        variant_id: null,
        sku: product.sku,
        product_name: product.name,
      });
    }

    for (const variant of safeArray(product.variants)) {
      if (variant.sku) {
        bySku.set(normalizeString(variant.sku), {
          product_id: product.id,
          variant_id: variant.id,
          sku: variant.sku,
          product_name: product.name,
        });
      }
    }
  }

  return { byProductId, bySku };
}

async function collectSales({
  days = 90,
  maxPagesOrders = 20,
} = {}) {
  const orders = await fetchRecentOrders({
    days,
    maxPages: maxPagesOrders,
  });

  const salesMap = new Map();
  const concurrency = 10;

  for (let i = 0; i < orders.length; i += concurrency) {
    const batch = orders.slice(i, i + concurrency);

    const batchItems = await Promise.all(
      batch.map(async (order) => ({
        orderId: order.id,
        items: await fetchOrderProducts(order.id),
      }))
    );

    for (const group of batchItems) {
      for (const item of group.items) {
        const productId = item.product_id != null ? String(item.product_id) : null;
        const sku = item.sku ? normalizeString(item.sku) : null;
        const key = sku ? `sku:${sku}` : `product:${productId}`;

        if (!salesMap.has(key)) {
          salesMap.set(key, {
            key,
            product_id: item.product_id ?? null,
            sku: item.sku || null,
            quantity_sold: 0,
            order_count: 0,
            revenue_ex_tax: 0,
            revenue_inc_tax: 0,
            order_ids: new Set(),
          });
        }

        const row = salesMap.get(key);
        const qty = toInt(item.quantity, 0);
        const priceEx = toFloat(item.price_ex_tax, 0) ?? 0;
        const priceInc = toFloat(item.price_inc_tax, 0) ?? 0;

        row.quantity_sold += qty;
        row.revenue_ex_tax += priceEx * qty;
        row.revenue_inc_tax += priceInc * qty;

        if (!row.order_ids.has(group.orderId)) {
          row.order_ids.add(group.orderId);
          row.order_count += 1;
        }
      }
    }
  }

  for (const row of salesMap.values()) {
    delete row.order_ids;
  }

  return { orders, salesMap };
}

function mergeSalesWithCatalog(products, salesMap) {
  const { byProductId, bySku } = buildProductLookup(products);
  const merged = [];

  for (const row of salesMap.values()) {
    let product = null;

    if (row.sku && bySku.has(normalizeString(row.sku))) {
      const ref = bySku.get(normalizeString(row.sku));
      product = byProductId.get(String(ref.product_id)) || null;

      merged.push({
        ...row,
        matched_by: "sku",
        variant_id: ref.variant_id ?? null,
        product_name: product?.name || ref.product_name || null,
        brand_id: product?.brand_id ?? null,
        categories: safeArray(product?.categories),
        price: product?.price ?? null,
        cost_price: product?.cost_price ?? null,
        is_visible: product?.is_visible ?? null,
      });
      continue;
    }

    if (row.product_id != null && byProductId.has(String(row.product_id))) {
      product = byProductId.get(String(row.product_id));

      merged.push({
        ...row,
        matched_by: "product_id",
        variant_id: null,
        product_name: product?.name || null,
        brand_id: product?.brand_id ?? null,
        categories: safeArray(product?.categories),
        price: product?.price ?? null,
        cost_price: product?.cost_price ?? null,
        is_visible: product?.is_visible ?? null,
      });
      continue;
    }

    merged.push({
      ...row,
      matched_by: "none",
      variant_id: null,
      product_name: null,
      brand_id: null,
      categories: [],
      price: null,
      cost_price: null,
      is_visible: null,
    });
  }

  return merged;
}

/* -------------------------------- Routes --------------------------------- */

app.get("/", (req, res) => {
  res.send("BigCommerce connector is running final 🚀");
});

app.get("/products", async (req, res) => {
  try {
    const page = toInt(req.query.page, 1);
    const limit = toInt(req.query.limit, 50);
    const sku = req.query.sku;
    const keyword = req.query.keyword;
    const category_id = req.query.category_id;
    const include = req.query.include || undefined;
    const bin = req.query.bin_picking_number;
    const allResults = String(req.query.all || "").toLowerCase() === "true";

    /*
      GPT / Admin lookup route.
      IMPORTANT:
      category_id filtering is handled locally because BigCommerce rejected
      category_id as a direct /v3/catalog/products filter in live use.
      This route does NOT affect storefront cart quantity logic.
    */

    if (bin) {
      const products = await fetchAllProducts({
        maxPages: toInt(req.query.max_pages_products, 100),
        include: include || "variants",
        keyword,
        category_id,
        sku,
      });

      const rows = flattenInventoryRows(products).filter((row) => {
        return normalizeString(row.bin_picking_number) === normalizeString(bin);
      });

      return res.json({
        mode: "bin_lookup",
        filters: {
          bin_picking_number: bin,
          keyword: keyword || null,
          category_id: category_id || null,
          sku: sku || null,
        },
        counts: {
          products_scanned: products.length,
          matches: rows.length,
        },
        data: rows,
      });
    }

    const maxPagesProducts = toInt(req.query.max_pages_products, 100);

    if (category_id) {
      const products = await fetchAllProducts({
        maxPages: maxPagesProducts,
        include: include || "variants",
        keyword,
        category_id,
        sku,
        limit: 250,
      });

      if (allResults) {
        return res.json({
          data: products,
          meta: {
            pagination: {
              total: products.length,
              count: products.length,
              per_page: products.length,
              current_page: 1,
              total_pages: 1,
              links: {
                current: `?all=true`,
              },
            },
          },
        });
      }

      const total = products.length;
      const start = Math.max((page - 1) * limit, 0);
      const end = start + limit;
      const paged = products.slice(start, end);
      const totalPages = Math.max(Math.ceil(total / limit), 1);

      return res.json({
        data: paged,
        meta: {
          pagination: {
            total,
            count: paged.length,
            per_page: limit,
            current_page: page,
            total_pages: totalPages,
            links: {
              current: `?page=${page}&limit=${limit}`,
            },
          },
        },
      });
    }

    const query = buildQuery({
      page,
      limit,
      sku,
      keyword,
      include,
    });

    const data = await bcRequest("GET", `/v3/catalog/products${query}`);
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/products/by-bin", async (req, res) => {
  try {
    const bin = req.query.bin_picking_number;
    if (!bin) {
      return res.status(400).json({
        error: "Missing required query param: bin_picking_number",
      });
    }

    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    const rows = flattenInventoryRows(products).filter((row) => {
      return normalizeString(row.bin_picking_number) === normalizeString(bin);
    });

    return res.json({
      filters: {
        bin_picking_number: bin,
        max_pages_products: maxPagesProducts,
      },
      counts: {
        products_scanned: products.length,
        matches: rows.length,
      },
      data: rows,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/product", async (req, res) => {
  try {
    const id = req.query.id;
    if (!id) {
      return res.status(400).json({ error: "Missing required query param: id" });
    }

    const include = req.query.include || "variants";
    const query = buildQuery({ include });
    const data = await bcRequest("GET", `/v3/catalog/products/${id}${query}`);
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.post("/products/update", async (req, res) => {
  try {
    const { product_id, payload } = req.body;

    if (!product_id || !payload || typeof payload !== "object") {
      return res.status(400).json({
        error: "Body must include product_id and payload object",
      });
    }

    const data = await bcRequest("PUT", `/v3/catalog/products/${product_id}`, payload);

    return res.json({
      success: true,
      product_id,
      payload,
      bigcommerce_response: data,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.post("/products/update-by-sku", async (req, res) => {
  try {
    const { sku, payload } = req.body;

    if (!sku || !payload || typeof payload !== "object") {
      return res.status(400).json({
        error: "Body must include sku and payload object",
      });
    }

    const result = await bcRequest(
      "GET",
      `/v3/catalog/products?limit=1&sku=${encodeURIComponent(sku)}`
    );

    const matches = safeArray(result.data);
    if (matches.length === 0) {
      return res.status(404).json({ error: `No product found for sku ${sku}` });
    }

    const product = matches[0];
    const data = await bcRequest("PUT", `/v3/catalog/products/${product.id}`, payload);

    return res.json({
      success: true,
      sku,
      product_id: product.id,
      payload,
      bigcommerce_response: data,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.post("/products/update-categories", async (req, res) => {
  try {
    const { product_id, categories } = req.body;

    if (!product_id || !Array.isArray(categories)) {
      return res.status(400).json({
        error: "Body must include product_id and categories[]",
      });
    }

    const data = await bcRequest("PUT", `/v3/catalog/products/${product_id}`, {
      categories,
    });

    return res.json({
      success: true,
      product_id,
      categories,
      bigcommerce_response: data,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/orders", async (req, res) => {
  try {
    const query = buildQuery({
      page: toInt(req.query.page, 1),
      limit: toInt(req.query.limit, 50),
      status_id: req.query.status_id,
      min_date_created: req.query.min_date_created,
      max_date_created: req.query.max_date_created,
      customer_id: req.query.customer_id,
    });

    const data = await bcRequest("GET", `/v2/orders${query}`);
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/order-products", async (req, res) => {
  try {
    const order_id = req.query.order_id;
    if (!order_id) {
      return res.status(400).json({ error: "Missing required query param: order_id" });
    }

    const data = await bcRequest("GET", `/v2/orders/${order_id}/products`);
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/customers", async (req, res) => {
  try {
    const page = toInt(req.query.page, 1);
    const limit = toInt(req.query.limit, 50);
    const email = req.query.email;
    const name = req.query.name;
    const group_id = req.query.group_id;

    let endpoint = `/v3/customers?page=${page}&limit=${limit}`;

    if (email) {
      endpoint += `&email:in=${encodeURIComponent(email)}`;
    }

    if (name) {
      endpoint += `&name:like=${encodeURIComponent(name)}`;
    }

    if (group_id) {
      endpoint += `&customer_group_id:in=${encodeURIComponent(group_id)}`;
    }

    const data = await bcRequest("GET", endpoint);
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.post("/customers/update", async (req, res) => {
  try {
    const { customer_id, payload } = req.body;

    if (!customer_id || !payload || typeof payload !== "object") {
      return res.status(400).json({
        error: "Body must include customer_id and payload object",
      });
    }

    const data = await bcRequest("PUT", `/v2/customers/${customer_id}`, payload);
    return res.json({
      success: true,
      customer_id,
      payload,
      bigcommerce_response: data,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.post("/customers/change-group", async (req, res) => {
  try {
    const { customer_id, customer_group_id } = req.body;

    if (!customer_id || customer_group_id === undefined) {
      return res.status(400).json({
        error: "Body must include customer_id and customer_group_id",
      });
    }

    const data = await bcRequest("PUT", `/v2/customers/${customer_id}`, {
      customer_group_id,
    });

    return res.json({
      success: true,
      customer_id,
      customer_group_id,
      bigcommerce_response: data,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/customer-groups", async (req, res) => {
  try {
    const data = await bcRequest("GET", "/v2/customer_groups");
    return res.json(data);
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get('/inventory', async (req, res) => {
  try {
    const { sku } = req.query;

    if (!sku) {
      return res.status(400).json({ error: 'Missing sku' });
    }

    const response = await bcRequest(
      'GET',
      `/v3/catalog/products?sku=${encodeURIComponent(sku)}`
    );

    const product = response?.data?.[0];

    if (!product) {
      return res.status(404).json({
        sku,
        inventory_level: 0,
        found: false
      });
    }

    return res.json({
      sku,
      inventory_level: Number(product.inventory_level ?? 0),
      found: true
    });
  } catch (error) {
    return res.status(500).json({
      error: error.message
    });
  }
});

/* ------------------------ GPT / Admin report routes ------------------------ */
/*
  The routes below are reporting/analytics helpers for the GPT/admin connector.
  Category filtering is handled locally after product fetches.
  These routes are safe to modify independently from storefront cart logic.
*/

app.get("/inventory-top", async (req, res) => {
  try {
    const limit = toInt(req.query.limit, 20);
    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const categoryId = req.query.category_id ? String(req.query.category_id) : null;
    const brandId = req.query.brand_id ? String(req.query.brand_id) : null;

    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    let rows = flattenInventoryRows(products).filter(
      (x) => typeof x.inventory_level === "number"
    );

    if (categoryId) {
      rows = rows.filter((x) => productInCategory(x, categoryId));
    }

    if (brandId) {
      rows = rows.filter((x) => String(x.brand_id) === brandId);
    }

    rows.sort((a, b) => (b.inventory_level ?? -1) - (a.inventory_level ?? -1));

    return res.json({
      filters: {
        limit,
        category_id: categoryId,
        brand_id: brandId,
        max_pages_products: maxPagesProducts,
      },
      counts: {
        products_scanned: products.length,
        inventory_rows_scanned: rows.length,
      },
      data: rows.slice(0, limit),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/dead-stock", async (req, res) => {
  try {
    const days = toInt(req.query.days, 90);
    const minInventory = toInt(req.query.min_inventory, 1);
    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const maxPagesOrders = toInt(req.query.max_pages_orders, 20);
    const categoryId = req.query.category_id ? String(req.query.category_id) : null;
    const brandId = req.query.brand_id ? String(req.query.brand_id) : null;

    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    let rows = flattenInventoryRows(products).filter(
      (x) => typeof x.inventory_level === "number" && x.inventory_level >= minInventory
    );

    if (categoryId) {
      rows = rows.filter((x) => productInCategory(x, categoryId));
    }

    if (brandId) {
      rows = rows.filter((x) => String(x.brand_id) === brandId);
    }

    const { salesMap, orders } = await collectSales({
      days,
      maxPagesOrders,
    });

    const soldKeys = new Set(salesMap.keys());

    const data = rows.filter((row) => {
      const sku = normalizeString(row.variant_sku || row.product_sku);
      const skuKey = sku ? `sku:${sku}` : null;
      const productKey = `product:${row.product_id}`;

      return !(skuKey && soldKeys.has(skuKey)) && !soldKeys.has(productKey);
    });

    return res.json({
      filters: {
        days,
        min_inventory: minInventory,
        category_id: categoryId,
        brand_id: brandId,
        max_pages_products: maxPagesProducts,
        max_pages_orders: maxPagesOrders,
      },
      counts: {
        products_scanned: products.length,
        orders_scanned: orders.length,
        dead_stock_count: data.length,
      },
      data: uniqueBy(data, (x) => `${x.type}:${x.product_id}:${x.variant_id || 0}`),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/zero-stock-stale", async (req, res) => {
  try {
    const days = toInt(req.query.days, 180);
    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const maxPagesOrders = toInt(req.query.max_pages_orders, 20);
    const categoryId = req.query.category_id ? String(req.query.category_id) : null;
    const brandId = req.query.brand_id ? String(req.query.brand_id) : null;

    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    let rows = flattenInventoryRows(products).filter(
      (x) => typeof x.inventory_level === "number" && x.inventory_level === 0
    );

    if (categoryId) {
      rows = rows.filter((x) => productInCategory(x, categoryId));
    }

    if (brandId) {
      rows = rows.filter((x) => String(x.brand_id) === brandId);
    }

    const { salesMap, orders } = await collectSales({
      days,
      maxPagesOrders,
    });

    const soldKeys = new Set(salesMap.keys());

    const data = rows.filter((row) => {
      const sku = normalizeString(row.variant_sku || row.product_sku);
      const skuKey = sku ? `sku:${sku}` : null;
      const productKey = `product:${row.product_id}`;

      return !(skuKey && soldKeys.has(skuKey)) && !soldKeys.has(productKey);
    });

    return res.json({
      filters: {
        days,
        category_id: categoryId,
        brand_id: brandId,
        max_pages_products: maxPagesProducts,
        max_pages_orders: maxPagesOrders,
      },
      counts: {
        products_scanned: products.length,
        orders_scanned: orders.length,
        stale_zero_stock_count: data.length,
      },
      data: uniqueBy(data, (x) => `${x.type}:${x.product_id}:${x.variant_id || 0}`),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/inactive-customers", async (req, res) => {
  try {
    const days = toInt(req.query.days, 90);
    const limit = toInt(req.query.limit, 250);
    const maxPagesCustomers = toInt(req.query.max_pages_customers, 20);
    const maxPagesOrders = toInt(req.query.max_pages_orders, 20);

    const customers = await fetchAllCustomers({
      limit,
      maxPages: maxPagesCustomers,
    });

    const recentOrders = await fetchRecentOrders({
      days,
      maxPages: maxPagesOrders,
    });

    const activeCustomerIds = new Set(
      recentOrders
        .map((o) => o.customer_id)
        .filter((id) => id !== null && id !== undefined)
        .map(String)
    );

    const data = customers.filter((c) => !activeCustomerIds.has(String(c.id)));

    return res.json({
      filters: {
        days,
        limit,
        max_pages_customers: maxPagesCustomers,
        max_pages_orders: maxPagesOrders,
      },
      counts: {
        customers_scanned: customers.length,
        recent_orders_scanned: recentOrders.length,
        inactive_customers_count: data.length,
      },
      data,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/top-sellers", async (req, res) => {
  try {
    const days = toInt(req.query.days, 90);
    const limit = toInt(req.query.limit, 20);
    const metric = req.query.metric === "revenue" ? "revenue" : "quantity";
    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const maxPagesOrders = toInt(req.query.max_pages_orders, 20);

    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    const { salesMap, orders } = await collectSales({
      days,
      maxPagesOrders,
    });

    let merged = mergeSalesWithCatalog(products, salesMap);

    merged.sort((a, b) => {
      if (metric === "revenue") {
        return (b.revenue_ex_tax ?? 0) - (a.revenue_ex_tax ?? 0);
      }
      return (b.quantity_sold ?? 0) - (a.quantity_sold ?? 0);
    });

    return res.json({
      filters: {
        days,
        limit,
        metric,
        max_pages_products: maxPagesProducts,
        max_pages_orders: maxPagesOrders,
      },
      counts: {
        products_scanned: products.length,
        orders_scanned: orders.length,
        rows_ranked: merged.length,
      },
      data: merged.slice(0, limit),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/top-sellers-by-brand", async (req, res) => {
  try {
    const brandId = req.query.brand_id;
    if (!brandId) {
      return res.status(400).json({ error: "Missing required query param: brand_id" });
    }

    const days = toInt(req.query.days, 90);
    const limit = toInt(req.query.limit, 20);
    const metric = req.query.metric === "revenue" ? "revenue" : "quantity";
    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const maxPagesOrders = toInt(req.query.max_pages_orders, 20);

    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    const { salesMap, orders } = await collectSales({
      days,
      maxPagesOrders,
    });

    let merged = mergeSalesWithCatalog(products, salesMap).filter(
      (x) => String(x.brand_id) === String(brandId)
    );

    merged.sort((a, b) => {
      if (metric === "revenue") {
        return (b.revenue_ex_tax ?? 0) - (a.revenue_ex_tax ?? 0);
      }
      return (b.quantity_sold ?? 0) - (a.quantity_sold ?? 0);
    });

    return res.json({
      filters: {
        brand_id: String(brandId),
        days,
        limit,
        metric,
        max_pages_products: maxPagesProducts,
        max_pages_orders: maxPagesOrders,
      },
      counts: {
        products_scanned: products.length,
        orders_scanned: orders.length,
        rows_ranked: merged.length,
      },
      data: merged.slice(0, limit),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/top-sellers-by-category", async (req, res) => {
  try {
    const categoryId = req.query.category_id;
    if (!categoryId) {
      return res.status(400).json({ error: "Missing required query param: category_id" });
    }

    const days = toInt(req.query.days, 90);
    const limit = toInt(req.query.limit, 20);
    const metric = req.query.metric === "revenue" ? "revenue" : "quantity";
    const maxPagesProducts = toInt(req.query.max_pages_products, 100);
    const maxPagesOrders = toInt(req.query.max_pages_orders, 20);

    const products = await fetchAllProducts({
      maxPages: maxPagesProducts,
      include: "variants",
    });

    const { salesMap, orders } = await collectSales({
      days,
      maxPagesOrders,
    });

    let merged = mergeSalesWithCatalog(products, salesMap).filter((x) =>
      safeArray(x.categories).map(String).includes(String(categoryId))
    );

    merged.sort((a, b) => {
      if (metric === "revenue") {
        return (b.revenue_ex_tax ?? 0) - (a.revenue_ex_tax ?? 0);
      }
      return (b.quantity_sold ?? 0) - (a.quantity_sold ?? 0);
    });

    return res.json({
      filters: {
        category_id: String(categoryId),
        days,
        limit,
        metric,
        max_pages_products: maxPagesProducts,
        max_pages_orders: maxPagesOrders,
      },
      counts: {
        products_scanned: products.length,
        orders_scanned: orders.length,
        rows_ranked: merged.length,
      },
      data: merged.slice(0, limit),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/* ------------------------ Category helper routes ------------------------ */
/*
  GPT / Admin helper routes.
  Safe to use for finding category IDs from SKU or category URL.
  These routes do NOT affect storefront cart quantity updates.
*/

app.get("/category-ids/by-sku", async (req, res) => {
  try {
    const sku = (req.query.sku || "").toString().trim();
    if (!sku) {
      return res.status(400).json({ error: "Missing required query param: sku" });
    }

    const products = await fetchAllProducts({
      sku,
      maxPages: 5,
      include: "variants",
    });

    const product =
      safeArray(products).find(
        (p) =>
          normalizeString(p.sku) === normalizeString(sku) ||
          normalizeString(p.upc) === normalizeString(sku)
      ) || safeArray(products)[0];

    if (!product) {
      return res.status(404).json({ error: "Product not found", sku });
    }

    return res.json({
      sku,
      product_id: product.id,
      name: product.name,
      custom_url: product.custom_url || null,
      categories: safeArray(product.categories),
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

app.get("/category/by-url", async (req, res) => {
  try {
    const inputUrl = (req.query.url || "").toString().trim();
    if (!inputUrl) {
      return res.status(400).json({ error: "Missing required query param: url" });
    }

    const targetPath = normalizePathFromUrl(inputUrl);
    const maxPagesCategories = toInt(req.query.max_pages_categories, 20);
    const categories = await fetchAllCategories({
      maxPages: maxPagesCategories,
    });

    const category = categories.find((cat) => {
      const categoryPath = normalizePathFromUrl(cat?.custom_url?.url || cat?.url || "");
      return categoryPath === targetPath;
    });

    if (!category) {
      return res.status(404).json({
        error: "Category not found for URL",
        url: inputUrl,
        normalized_path: targetPath,
      });
    }

    return res.json({
      url: inputUrl,
      normalized_path: targetPath,
      data: category,
    });
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

/* ---------------------- CART / STOREFRONT SAFE ROUTE ---------------------- */
/*
  DO NOT MODIFY without testing storefront cart behavior.
  This route is separate from GPT/admin reporting logic and does not depend on
  fetchAllProducts() or category-based catalog analytics.
*/

app.post("/cart-inventory-check", async (req, res) => {
  try {
    const skus = Array.isArray(req.body?.skus) ? req.body.skus : [];

    const cleanedSkus = [...new Set(
      skus
        .map((sku) => (sku || "").toString().trim())
        .filter(Boolean)
    )];

    if (!cleanedSkus.length) {
      return res.json({ ok: true, inventory: {} });
    }

    const query = buildQuery({
      "sku:in": cleanedSkus.join(","),
      limit: 250,
    });

    const result = await bcRequest("GET", `/v3/inventory/items${query}`);
    const rows = safeArray(result.data);

    const inventory = {};

    for (const row of rows) {
      const sku = row?.sku || row?.identity?.sku || null;
      if (!sku) continue;

      let available = null;

      if (typeof row.available_to_sell === "number") {
        available = row.available_to_sell;
      } else if (Array.isArray(row.location_inventory)) {
        available = row.location_inventory.reduce((sum, loc) => {
          return sum + (typeof loc.available_to_sell === "number" ? loc.available_to_sell : 0);
        }, 0);
      }

      inventory[sku] = {
        sku,
        available_to_sell: available,
      };
    }

    return res.json({
      ok: true,
      inventory,
    });
  } catch (err) {
    return res.status(500).json({
      ok: false,
      error: err.message,
    });
  }
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Server running on port ${PORT}`);
});
