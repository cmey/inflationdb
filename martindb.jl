# martindb contains price over time for household items in multiple stores.
# Plot the evolution of prices over time for common items (look for inflation).
using LibPQ, DataFrames, Plots, Dates, StatsPlots, StatsBase, SQLStrings

# utils
function check(result)
    @assert status(result) == LibPQ.libpq_c.PGRES_TUPLES_OK && isempty(LibPQ.error_message(result))  LibPQ.error_message(result)
end

function runquery(conn, sql::SQLStrings.Sql)
    query, args = SQLStrings.prepare(sql)
    LibPQ.execute(conn, query, args)
end

# connect to pg, no password, run pg with POSTGRES_HOST_AUTH_METHOD=trust
conn = LibPQ.Connection("postgresql://postgres@localhost:5433")

# list tables
result = execute(conn, """
SELECT *
FROM pg_catalog.pg_tables
WHERE schemaname != 'pg_catalog' AND 
    schemaname != 'information_schema';
""")
check(result)
df = result |> DataFrame
show(df, allrows=true)

# load some tables
result = execute(conn, """SELECT * FROM vendor """)
check(result)
df_vendor = result |> DataFrame
show(df_vendor)

result = execute(conn, """SELECT * FROM product """)
check(result)
df_product = result |> DataFrame
show(df_product)

result = execute(conn, """SELECT * FROM label """)
check(result)
df_label = result |> DataFrame
show(df_label)

result = execute(conn, """SELECT * FROM price """)
check(result)
df_price = result |> DataFrame
show(df_price)

# some fields are NULL in SQL, missing in Julia
dropmissing!(df_price, :product_id)
dropmissing!(df_price, :vendor_id)

# find most common products (items)
cm_p = countmap(df_price.product_id)
scm_p = sort(collect(cm_p), by = x -> x[2], rev=true)
top_products = scm_p[1:10]  # top N

# find most common vendors (stores)
cm_v = countmap(df_price.vendor_id)
scm_v = sort(collect(cm_v), by = x -> x[2], rev=true)
top_vendors = scm_v[1:10]  # top N
top_vendor_ids = first.(top_vendors)

# make one plot per product, each containing multiple series (one for each top vendor) 
for product_id_pair in top_products
    product_id = product_id_pair[1]
    df = filter(:id => ==(product_id), df_product)
    product_name = df[1, :name]

    # get price timeseries for this product, for all top vendors
    #   ignore promotions
    result = runquery(conn, sql```
    SELECT update_time, amount, vendor_id
    FROM price
    WHERE product_id = $product_id
    AND promotion IS NULL
    AND vendor_id IN ($(top_vendor_ids...))
    ORDER BY update_time ASC;
    ```)
    check(result)
    df = result |> DataFrame
    show(df)
    
    # parse timestamps
    dropmissing!(df)
    transform!(df, :update_time .=> ByRow(x -> x[3:21]); renamecols = false)
    transform!(df, :update_time .=> ByRow(x -> DateTime(x, dateformat"y-m-d H:M:S")); renamecols = false)
    df.amount = Float32.(df.amount)

    # pivot table into timeseries for each top vendors
    df_u = unstack(df, :update_time, :vendor_id, :amount; allowduplicates=true)
    
    # plot price timeseries for each vendor
    y_col_names = names(df_u, Not(:update_time))
    p = nothing
    for y_col_name in y_col_names
        vendor_name = (df_vendor.name[df_vendor.id .== y_col_name])[1]
        y = df_u[:, y_col_name]
        # since not all vendors have price at the same timestamp, pivot table contains a lot of missing data
        keeps = map(!ismissing, y)  # keep only not missing x,y pairs
        if y_col_name == y_col_names[1]
            p = plot(df_u.update_time[keeps], y[keeps], title = product_name, label = vendor_name, legend=:bottomright)
        else
            plot!(p, df_u.update_time[keeps], y[keeps], label = vendor_name)
        end
    end
    display(p)
end
