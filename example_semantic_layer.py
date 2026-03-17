"""PyRel model: coffee_shop_vp_ops"""
from relationalai.semantics import Model, Bool, Float, Integer, String

# Apply extension to your model


model = Model("coffee_shop_vp_ops")


# ── Source Tables ────────────────────────────────────────────────
class Sources:
    class bespoke:
        class coffee_shop_vp_ops:
            baristas = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.BARISTAS")
            inventory_items = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.INVENTORY_ITEMS")
            inventory_transactions = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.INVENTORY_TRANSACTIONS")
            location_daily_metrics = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.LOCATION_DAILY_METRICS")
            locations = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.LOCATIONS")
            loyalty_members = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.LOYALTY_MEMBERS")
            menu_items = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.MENU_ITEMS")
            order_items = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.ORDER_ITEMS")
            orders = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.ORDERS")
            shifts = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.SHIFTS")
            supply_transfers = model.Table("BESPOKE.COFFEE_SHOP_VP_OPS.SUPPLY_TRANSFERS")

# ── Concepts ─────────────────────────────────────────────────────
MenuItem = model.Concept("MenuItem", identify_by={"Menu Item ID": Integer})
model.define(MenuItem.new(menu_item_id=Sources.bespoke.coffee_shop_vp_ops.menu_items.menu_item_id))

InventoryItem = model.Concept("InventoryItem", identify_by={"Inventory Item ID": Integer})
model.define(InventoryItem.new(inventory_item_id=Sources.bespoke.coffee_shop_vp_ops.inventory_items.inventory_item_id))

InventoryTransaction = model.Concept("InventoryTransaction", identify_by={"Inventory Transaction ID": Integer})
model.define(InventoryTransaction.new(inventory_txn_id=Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.inventory_txn_id))

Shift = model.Concept("Shift", identify_by={"Shift ID": Integer})
model.define(Shift.new(shift_id=Sources.bespoke.coffee_shop_vp_ops.shifts.shift_id))

OrderItem = model.Concept("OrderItem", identify_by={"Order Item ID": Integer})
model.define(OrderItem.new(order_item_id=Sources.bespoke.coffee_shop_vp_ops.order_items.order_item_id))

LoyaltyMember = model.Concept("LoyaltyMember", identify_by={"Member ID": Integer})
model.define(LoyaltyMember.new(member_id=Sources.bespoke.coffee_shop_vp_ops.loyalty_members.member_id))

LocationDailyMetrics = model.Concept("LocationDailyMetrics", identify_by={"Metric ID": Integer, "Location ID": Integer, "Metric Date": String})
model.define(LocationDailyMetrics.new(metric_id=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.metric_id))

Location = model.Concept("Location", identify_by={"Location ID": Integer})
model.define(Location.new(location_id=Sources.bespoke.coffee_shop_vp_ops.locations.location_id))

SupplyTransfer = model.Concept("SupplyTransfer", identify_by={"Transfer ID": Integer})
model.define(SupplyTransfer.new(transfer_id=Sources.bespoke.coffee_shop_vp_ops.supply_transfers.transfer_id))

Order = model.Concept("Order", identify_by={"Order ID": Integer})
model.define(Order.new(order_id=Sources.bespoke.coffee_shop_vp_ops.orders.order_id))

Barista = model.Concept("Barista", identify_by={"Barista ID": Integer})
model.define(Barista.new(barista_id=Sources.bespoke.coffee_shop_vp_ops.baristas.barista_id))


# ── Properties & Relationships ───────────────────────────────────
MenuItem.financials_operations = model.Relationship(f"{MenuItem} has {Float:Retail Price} has {Float:Ingredient Cost} has {Integer:Prep Time (Seconds)}", short_name="menu_item_financials_operations")
MenuItem.descriptors = model.Relationship(f"{MenuItem} has {String:Item Name} has {String:Category} has {String:Size} has {Integer:Calories} has {Bool:Is Seasonal} has {Bool:Is Available}", short_name="menu_item_descriptors")
model.define(MenuItem.filter_by(Menu Item ID=Sources.bespoke.coffee_shop_vp_ops.menu_items.Menu Item ID).financials_operations(Sources.bespoke.coffee_shop_vp_ops.menu_items.menu_item_id, Sources.bespoke.coffee_shop_vp_ops.menu_items.retail_price, Sources.bespoke.coffee_shop_vp_ops.menu_items.ingredient_cost, Sources.bespoke.coffee_shop_vp_ops.menu_items.prep_time_seconds))
model.define(MenuItem.filter_by(Menu Item ID=Sources.bespoke.coffee_shop_vp_ops.menu_items.Menu Item ID).descriptors(Sources.bespoke.coffee_shop_vp_ops.menu_items.menu_item_id, Sources.bespoke.coffee_shop_vp_ops.menu_items.item_name, Sources.bespoke.coffee_shop_vp_ops.menu_items.category, Sources.bespoke.coffee_shop_vp_ops.menu_items.size, Sources.bespoke.coffee_shop_vp_ops.menu_items.calories, Sources.bespoke.coffee_shop_vp_ops.menu_items.is_seasonal, Sources.bespoke.coffee_shop_vp_ops.menu_items.is_available))

InventoryItem.descriptors = model.Relationship(f"{InventoryItem} has {String:Item Name} has {String:Category} has {String:Unit of Measure} has {Bool:Is Perishable} has {Integer:Shelf Life Days}", short_name="inventory_item_descriptors")
InventoryItem.procurement_attributes = model.Relationship(f"{InventoryItem} has {Float:Cost Per Unit} has {Integer:Reorder Point} has {Integer:Optimal Order Quantity}", short_name="inventory_item_procurement_attributes")
model.define(InventoryItem.filter_by(Inventory Item ID=Sources.bespoke.coffee_shop_vp_ops.inventory_items.Inventory Item ID).descriptors(Sources.bespoke.coffee_shop_vp_ops.inventory_items.inventory_item_id, Sources.bespoke.coffee_shop_vp_ops.inventory_items.item_name, Sources.bespoke.coffee_shop_vp_ops.inventory_items.category, Sources.bespoke.coffee_shop_vp_ops.inventory_items.unit_of_measure, Sources.bespoke.coffee_shop_vp_ops.inventory_items.is_perishable, Sources.bespoke.coffee_shop_vp_ops.inventory_items.shelf_life_days))
model.define(InventoryItem.filter_by(Inventory Item ID=Sources.bespoke.coffee_shop_vp_ops.inventory_items.Inventory Item ID).procurement_attributes(Sources.bespoke.coffee_shop_vp_ops.inventory_items.inventory_item_id, Sources.bespoke.coffee_shop_vp_ops.inventory_items.cost_per_unit, Sources.bespoke.coffee_shop_vp_ops.inventory_items.reorder_point, Sources.bespoke.coffee_shop_vp_ops.inventory_items.optimal_order_quantity))

InventoryTransaction.at_location = model.Relationship(f"{InventoryTransaction} occurred {Location}", short_name="inventory_transaction_at_location")
InventoryTransaction.involves_inventory_item = model.Relationship(f"{InventoryTransaction} affects {InventoryItem}", short_name="inventory_transaction_involves_inventory_item")
model.define(InventoryTransaction.filter_by(Inventory Transaction ID=Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.Inventory Transaction ID).at_location(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.Location ID), Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.inventory_txn_id, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.location_id, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.transaction_date, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.transaction_type, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.quantity, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.unit_cost, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.total_cost, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.waste_reason))
model.define(InventoryTransaction.filter_by(Inventory Transaction ID=Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.Inventory Transaction ID).involves_inventory_item(InventoryItem.filter_by(Inventory Item ID=Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.Inventory Item ID), Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.inventory_txn_id, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.inventory_item_id, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.transaction_date, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.transaction_type, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.quantity, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.unit_cost, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.total_cost, Sources.bespoke.coffee_shop_vp_ops.inventory_transactions.waste_reason))

Shift.at_location = model.Relationship(f"{Shift} worked {Location}", short_name="shift_at_location")
model.define(Shift.filter_by(Shift ID=Sources.bespoke.coffee_shop_vp_ops.shifts.Shift ID).at_location(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.shifts.Location ID), Sources.bespoke.coffee_shop_vp_ops.shifts.shift_id, Sources.bespoke.coffee_shop_vp_ops.shifts.location_id, Sources.bespoke.coffee_shop_vp_ops.shifts.shift_date, Sources.bespoke.coffee_shop_vp_ops.shifts.shift_type, Sources.bespoke.coffee_shop_vp_ops.shifts.scheduled_hours, Sources.bespoke.coffee_shop_vp_ops.shifts.actual_hours, Sources.bespoke.coffee_shop_vp_ops.shifts.overtime_hours, Sources.bespoke.coffee_shop_vp_ops.shifts.labor_cost, Sources.bespoke.coffee_shop_vp_ops.shifts.orders_handled, Sources.bespoke.coffee_shop_vp_ops.shifts.was_no_show))

OrderItem.belongs_to_order = model.Relationship(f"{OrderItem} belongs to {Order}", short_name="order_item_belongs_to_order")
OrderItem.menu_item = model.Relationship(f"{OrderItem} represents {MenuItem}", short_name="order_item_menu_item")
model.define(OrderItem.filter_by(Order Item ID=Sources.bespoke.coffee_shop_vp_ops.order_items.Order Item ID).belongs_to_order(Order.filter_by(Order ID=Sources.bespoke.coffee_shop_vp_ops.order_items.Order ID), Sources.bespoke.coffee_shop_vp_ops.order_items.order_item_id, Sources.bespoke.coffee_shop_vp_ops.order_items.order_id))
model.define(OrderItem.filter_by(Order Item ID=Sources.bespoke.coffee_shop_vp_ops.order_items.Order Item ID).menu_item(MenuItem.filter_by(Menu Item ID=Sources.bespoke.coffee_shop_vp_ops.order_items.Menu Item ID), Sources.bespoke.coffee_shop_vp_ops.order_items.order_item_id, Sources.bespoke.coffee_shop_vp_ops.order_items.menu_item_id, Sources.bespoke.coffee_shop_vp_ops.order_items.quantity, Sources.bespoke.coffee_shop_vp_ops.order_items.unit_price, Sources.bespoke.coffee_shop_vp_ops.order_items.customization))

LoyaltyMember.preferred_location = model.Relationship(f"{LoyaltyMember} prefers {Location}", short_name="loyalty_member_preferred_location")
LoyaltyMember.order = model.Relationship(f"{LoyaltyMember} placed {Order}", short_name="loyalty_member_order")
LoyaltyMember.profile = model.Relationship(f"{LoyaltyMember} has {String:Full Name} has {String:Email} has {String:Phone} has {String:Enrollment Date} has {String:Tier} has {Bool:Is Mobile App User}", short_name="loyalty_member_profile")
LoyaltyMember.engagement_value_metrics = model.Relationship(f"{LoyaltyMember} has {Integer:Total Points} has {Float:Lifetime Spend} has {Integer:Visits Last 30 Days} has {Float:Churn Risk Score}", short_name="loyalty_member_engagement_value_metrics")
model.define(LoyaltyMember.filter_by(Member ID=Sources.bespoke.coffee_shop_vp_ops.loyalty_members.Member ID).preferred_location(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.loyalty_members.Location ID), Sources.bespoke.coffee_shop_vp_ops.loyalty_members.member_id, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.preferred_location_id))
model.define(LoyaltyMember.filter_by(Member ID=Sources.bespoke.coffee_shop_vp_ops.orders.Member ID).order(Order.filter_by(Order ID=Sources.bespoke.coffee_shop_vp_ops.orders.Order ID), Sources.bespoke.coffee_shop_vp_ops.orders.loyalty_member_id, Sources.bespoke.coffee_shop_vp_ops.orders.order_id, Sources.bespoke.coffee_shop_vp_ops.orders.order_date, Sources.bespoke.coffee_shop_vp_ops.orders.order_channel, Sources.bespoke.coffee_shop_vp_ops.orders.payment_method, Sources.bespoke.coffee_shop_vp_ops.orders.discount_amount, Sources.bespoke.coffee_shop_vp_ops.orders.total_amount))
model.define(LoyaltyMember.filter_by(Member ID=Sources.bespoke.coffee_shop_vp_ops.loyalty_members.Member ID).profile(Sources.bespoke.coffee_shop_vp_ops.loyalty_members.member_id, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.full_name, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.email, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.phone, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.enrollment_date, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.tier, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.is_mobile_app_user))
model.define(LoyaltyMember.filter_by(Member ID=Sources.bespoke.coffee_shop_vp_ops.loyalty_members.Member ID).engagement_value_metrics(Sources.bespoke.coffee_shop_vp_ops.loyalty_members.member_id, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.total_points, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.lifetime_spend, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.visits_last_30_days, Sources.bespoke.coffee_shop_vp_ops.loyalty_members.churn_risk_score))

LocationDailyMetrics.recorded_for_a_location = model.Relationship(f"{LocationDailyMetrics} recorded for {Location}", short_name="location_daily_metrics_recorded_for_a_location")
LocationDailyMetrics.operational_experience = model.Relationship(f"{LocationDailyMetrics} has {Integer:Total Orders} has {Integer:Staff Count} has {Integer:Avg Wait Time (Seconds)} has {Float:Customer Satisfaction Avg} has {Float:Mobile Order Pct}", short_name="location_daily_metrics_operational_experience")
LocationDailyMetrics.forecast = model.Relationship(f"{LocationDailyMetrics} has {Integer:Predicted Next Day Orders}", short_name="location_daily_metrics_forecast")
LocationDailyMetrics.financial_summary = model.Relationship(f"{LocationDailyMetrics} has {Float:Total Revenue} has {Float:Total COGS} has {Float:Total Labor Cost} has {Float:Waste Cost}", short_name="location_daily_metrics_financial_summary")
model.define(LocationDailyMetrics.filter_by(Metric ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric ID, Location ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Location ID, Metric Date=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric Date).recorded_for_a_location(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Location ID), Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.metric_id, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.location_id))
model.define(LocationDailyMetrics.filter_by(Metric ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric ID, Location ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Location ID, Metric Date=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric Date).operational_experience(Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.metric_id, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.total_orders, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.staff_count, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.avg_wait_time_seconds, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.customer_satisfaction_avg, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.mobile_order_pct))
model.define(LocationDailyMetrics.filter_by(Metric ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric ID, Location ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Location ID, Metric Date=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric Date).forecast(Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.metric_id, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.predicted_next_day_orders))
model.define(LocationDailyMetrics.filter_by(Metric ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric ID, Location ID=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Location ID, Metric Date=Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.Metric Date).financial_summary(Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.metric_id, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.total_revenue, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.total_cogs, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.total_labor_cost, Sources.bespoke.coffee_shop_vp_ops.location_daily_metrics.waste_cost))

Location.financial_operational_attributes = model.Relationship(f"{Location} has {Float:Monthly Rent} has {Float:Monthly Utility Cost} has {Integer:Daily Foot Traffic Average}", short_name="location_financial_operational_attributes")
Location.profile = model.Relationship(f"{Location} has {String:Store Name} has {String:City} has {String:State} has {String:Store Type} has {String:Opened Date} has {Bool:Is Active} has {Integer:Square Footage} has {Integer:Seating Capacity}", short_name="location_profile")
model.define(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.locations.Location ID).financial_operational_attributes(Sources.bespoke.coffee_shop_vp_ops.locations.location_id, Sources.bespoke.coffee_shop_vp_ops.locations.monthly_rent, Sources.bespoke.coffee_shop_vp_ops.locations.monthly_utility_cost, Sources.bespoke.coffee_shop_vp_ops.locations.daily_foot_traffic_avg))
model.define(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.locations.Location ID).profile(Sources.bespoke.coffee_shop_vp_ops.locations.location_id, Sources.bespoke.coffee_shop_vp_ops.locations.store_name, Sources.bespoke.coffee_shop_vp_ops.locations.city, Sources.bespoke.coffee_shop_vp_ops.locations.state, Sources.bespoke.coffee_shop_vp_ops.locations.store_type, Sources.bespoke.coffee_shop_vp_ops.locations.opened_date, Sources.bespoke.coffee_shop_vp_ops.locations.is_active, Sources.bespoke.coffee_shop_vp_ops.locations.square_footage, Sources.bespoke.coffee_shop_vp_ops.locations.seating_capacity))

SupplyTransfer.inventory_item_from_location_to_location = model.Relationship(f"{SupplyTransfer} moves {InventoryItem} via {Location}", short_name="supply_transfer_inventory_item_from_location_to_location")
model.define(SupplyTransfer.filter_by(Transfer ID=Sources.bespoke.coffee_shop_vp_ops.supply_transfers.Transfer ID).inventory_item_from_location_to_location(InventoryItem.filter_by(Inventory Item ID=Sources.bespoke.coffee_shop_vp_ops.supply_transfers.Inventory Item ID), Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.supply_transfers.Location ID), Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.supply_transfers.Location ID), Sources.bespoke.coffee_shop_vp_ops.supply_transfers.transfer_id, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.inventory_item_id, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.from_location_id, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.to_location_id, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.transfer_date, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.quantity, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.transfer_cost, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.reason, Sources.bespoke.coffee_shop_vp_ops.supply_transfers.delivery_time_hours))

Order.handled_by_barista = model.Relationship(f"{Order} handled by {Barista}", short_name="order_handled_by_barista")
Order.at_location = model.Relationship(f"{Order} placed {Location}", short_name="order_at_location")
Order.financials_metadata = model.Relationship(f"{Order} has {String:Order UUID} has {Float:Subtotal} has {Float:Tax Amount} has {Integer:Item Count} has {Integer:Wait Time (Seconds)}", short_name="order_financials_metadata")
model.define(Order.filter_by(Order ID=Sources.bespoke.coffee_shop_vp_ops.orders.Order ID).handled_by_barista(Barista.filter_by(Barista ID=Sources.bespoke.coffee_shop_vp_ops.orders.Barista ID), Sources.bespoke.coffee_shop_vp_ops.orders.order_id, Sources.bespoke.coffee_shop_vp_ops.orders.barista_id))
model.define(Order.filter_by(Order ID=Sources.bespoke.coffee_shop_vp_ops.orders.Order ID).at_location(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.orders.Location ID), Sources.bespoke.coffee_shop_vp_ops.orders.order_id, Sources.bespoke.coffee_shop_vp_ops.orders.location_id, Sources.bespoke.coffee_shop_vp_ops.orders.order_date, Sources.bespoke.coffee_shop_vp_ops.orders.order_channel, Sources.bespoke.coffee_shop_vp_ops.orders.hour_of_day))
model.define(Order.filter_by(Order ID=Sources.bespoke.coffee_shop_vp_ops.orders.Order ID).financials_metadata(Sources.bespoke.coffee_shop_vp_ops.orders.order_id, Sources.bespoke.coffee_shop_vp_ops.orders.order_uuid, Sources.bespoke.coffee_shop_vp_ops.orders.subtotal, Sources.bespoke.coffee_shop_vp_ops.orders.tax_amount, Sources.bespoke.coffee_shop_vp_ops.orders.item_count, Sources.bespoke.coffee_shop_vp_ops.orders.wait_time_seconds))

Barista.shift = model.Relationship(f"{Barista} worked {Shift} in {Location}", short_name="barista_shift")
Barista.home_location = model.Relationship(f"{Barista} works from {Location}", short_name="barista_home_location")
Barista.performance_metrics = model.Relationship(f"{Barista} has {Float:Avg Drinks Per Hour} has {Float:Customer Satisfaction Score}", short_name="barista_performance_metrics")
Barista.profile = model.Relationship(f"{Barista} has {String:Full Name} has {String:Email} has {String:Phone} has {String:Role} has {String:Hire Date} has {String:Employment Type} has {Bool:Is Active} has {Float:Hourly Wage}", short_name="barista_profile")
model.define(Barista.filter_by(Barista ID=Sources.bespoke.coffee_shop_vp_ops.shifts.Barista ID).shift(Shift.filter_by(Shift ID=Sources.bespoke.coffee_shop_vp_ops.shifts.Shift ID), Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.shifts.Location ID), Sources.bespoke.coffee_shop_vp_ops.shifts.barista_id, Sources.bespoke.coffee_shop_vp_ops.shifts.shift_id, Sources.bespoke.coffee_shop_vp_ops.shifts.shift_date, Sources.bespoke.coffee_shop_vp_ops.shifts.shift_type, Sources.bespoke.coffee_shop_vp_ops.shifts.scheduled_hours, Sources.bespoke.coffee_shop_vp_ops.shifts.actual_hours, Sources.bespoke.coffee_shop_vp_ops.shifts.overtime_hours, Sources.bespoke.coffee_shop_vp_ops.shifts.labor_cost, Sources.bespoke.coffee_shop_vp_ops.shifts.orders_handled, Sources.bespoke.coffee_shop_vp_ops.shifts.was_no_show, Sources.bespoke.coffee_shop_vp_ops.shifts.location_id))
model.define(Barista.filter_by(Barista ID=Sources.bespoke.coffee_shop_vp_ops.baristas.Barista ID).home_location(Location.filter_by(Location ID=Sources.bespoke.coffee_shop_vp_ops.baristas.Location ID), Sources.bespoke.coffee_shop_vp_ops.baristas.barista_id, Sources.bespoke.coffee_shop_vp_ops.baristas.home_location_id))
model.define(Barista.filter_by(Barista ID=Sources.bespoke.coffee_shop_vp_ops.baristas.Barista ID).performance_metrics(Sources.bespoke.coffee_shop_vp_ops.baristas.barista_id, Sources.bespoke.coffee_shop_vp_ops.baristas.avg_drinks_per_hour, Sources.bespoke.coffee_shop_vp_ops.baristas.customer_satisfaction_score))
model.define(Barista.filter_by(Barista ID=Sources.bespoke.coffee_shop_vp_ops.baristas.Barista ID).profile(Sources.bespoke.coffee_shop_vp_ops.baristas.barista_id, Sources.bespoke.coffee_shop_vp_ops.baristas.full_name, Sources.bespoke.coffee_shop_vp_ops.baristas.email, Sources.bespoke.coffee_shop_vp_ops.baristas.phone, Sources.bespoke.coffee_shop_vp_ops.baristas.role, Sources.bespoke.coffee_shop_vp_ops.baristas.hire_date, Sources.bespoke.coffee_shop_vp_ops.baristas.employment_type, Sources.bespoke.coffee_shop_vp_ops.baristas.is_active, Sources.bespoke.coffee_shop_vp_ops.baristas.hourly_wage))


# ====================================================================
# Business Rules Extension
# Generated by Business Rules Builder
# Rules: Barista.valid_hourly_wage, InventoryItem.valid_cost_per_unit, Location.valid_operating_costs, LoyaltyMember.valid_email_format, MenuItem.positive_pricing, Order.valid_order_amounts, Barista.years_of_service, InventoryItem.days_until_expiry, Location.years_in_operation, LocationDailyMetrics.gross_profit, LocationDailyMetrics.labor_cost_percentage, LoyaltyMember.average_order_value, MenuItem.profit_margin, Order.total_amount, InventoryItem.reorder_point_validation, LocationDailyMetrics.avg_wait_time_threshold, MenuItem.ingredient_cost_validation, Barista.monthly_orders_handled, Location.monthly_revenue, LoyaltyMember.is_recent_customer, Order.is_rush_hour_order, Barista.valid_hourly_wage, InventoryItem.valid_cost_per_unit, Location.valid_operating_costs, LoyaltyMember.valid_email_format, MenuItem.positive_pricing, Order.valid_order_amounts, Barista.years_of_service, InventoryItem.days_until_expiry, Location.years_in_operation, LocationDailyMetrics.gross_profit, LocationDailyMetrics.labor_cost_percentage, LoyaltyMember.average_order_value, MenuItem.profit_margin, Order.total_amount, InventoryItem.reorder_point_validation, LocationDailyMetrics.avg_wait_time_threshold, MenuItem.ingredient_cost_validation, Barista.monthly_orders_handled, Location.monthly_revenue, LoyaltyMember.is_recent_customer, Order.is_rush_hour_order, Shift.violates_overtime_limit
# ====================================================================

from typing import Optional
from datetime import date, datetime
from relationalai.semantics import define, Concept


def extend(model, concepts, source_tables):
    """
    Business rules extension for semantic layer.

    Generated by Business Rules Builder.
    """

    # Extensions to Existing Concepts
    @define(concepts.Barista)
    class BaristaExtension:
        @property
        def valid_hourly_wage(self) -> bool:
            """Returns True if the hourly wage is positive and within reasonable range (0-100)."""
            return (
                self.hourly_wage is not None
                and self.hourly_wage > 0
                and self.hourly_wage <= 100
            )

        @property
        def years_of_service(self) -> float:
            """Calculate years of service from hire date to current date."""
            from datetime import datetime

            if not self.hire_date:
                return 0.0

            try:
                hire_datetime = datetime.strptime(self.hire_date, "%Y-%m-%d")
                days_diff = (datetime.now() - hire_datetime).days
                return days_diff / 365.0
            except (ValueError, TypeError):
                return 0.0

        @property
        def monthly_orders_handled(self) -> int:
            """Count of total orders handled by this barista in the current month."""
            from datetime import datetime

            current_date = datetime.now()
            start_of_month = current_date.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )

            return sum(
                1
                for shift in self.shift
                if shift.orders
                and any(
                    order.order_date >= start_of_month and order.barista == self
                    for order in shift.orders
                )
            )

    @define(concepts.InventoryItem)
    class InventoryItemExtension:
        @property
        def valid_cost_per_unit(self) -> bool:
            """Returns True if the cost per unit is positive, False otherwise."""
            return self.cost_per_unit is not None and self.cost_per_unit > 0

        @property
        def days_until_expiry(self) -> Optional[int]:
            """Calculate days until expiry for perishable items based on shelf life and days since received."""
            if not self.is_perishable:
                return None
            return self.shelf_life_days - self.days_since_received

        @property
        def reorder_point_validation(self) -> bool:
            """Returns True if reorder point is less than optimal order quantity, False otherwise."""
            return self.reorder_point < self.optimal_order_quantity

    @define(concepts.Location)
    class LocationExtension:
        @property
        def valid_operating_costs(self) -> bool:
            """Returns True if both monthly rent and utility costs are positive values."""
            return (self.monthly_rent or 0) > 0 and (self.monthly_utility_cost or 0) > 0

        @property
        def years_in_operation(self) -> float:
            """Calculate the number of years since the location opened based on the opened date."""
            from datetime import datetime

            if not self.opened_date:
                return 0.0

            try:
                opened_datetime = datetime.strptime(self.opened_date, "%Y-%m-%d")
                current_datetime = datetime.now()
                days_diff = (current_datetime - opened_datetime).days
                return days_diff / 365.0
            except (ValueError, TypeError):
                return 0.0

        @property
        def monthly_revenue(self) -> float:
            """Sum of total revenue for this location in the current month."""
            from datetime import datetime

            current_date = datetime.now()
            start_of_month = current_date.replace(
                day=1, hour=0, minute=0, second=0, microsecond=0
            )

            return sum(
                order.order_total
                for order in self.orders
                if order.order_date >= start_of_month
            )

    @define(concepts.LoyaltyMember)
    class LoyaltyMemberExtension:
        @property
        def valid_email_format(self) -> bool:
            """Returns True if email is not null and matches valid email format pattern."""
            import re

            if self.email is None:
                return False

            email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            return re.match(email_pattern, self.email) is not None

        @property
        def average_order_value(self) -> float:
            """Calculate average spend per visit over lifetime by dividing lifetime spend by total number of orders."""
            total_orders = len(self.order)
            if total_orders == 0:
                return 0.0
            return self.lifetime_spend / total_orders

        @property
        def is_recent_customer(self) -> bool:
            """Returns True if the customer has visited in the last 30 days."""
            return (self.visits_last_30_days or 0) > 0

    @define(concepts.MenuItem)
    class MenuItemExtension:
        @property
        def positive_pricing(self) -> bool:
            """Returns True if both retail price and ingredient cost are positive values."""
            return (self.retail_price or 0) > 0 and (self.ingredient_cost or 0) > 0

        @property
        def profit_margin(self) -> float:
            """Calculate profit margin as percentage of retail price."""
            if self.retail_price is None or self.retail_price <= 0:
                return 0.0
            if self.ingredient_cost is None:
                return 100.0
            return (
                (self.retail_price - self.ingredient_cost) / self.retail_price
            ) * 100

        @property
        def ingredient_cost_validation(self) -> bool:
            """Returns True if ingredient cost does not exceed 40% of retail price."""
            if self.retail_price is None or self.ingredient_cost is None:
                return False
            return self.ingredient_cost <= (self.retail_price * 0.4)

    @define(concepts.Order)
    class OrderExtension:
        @property
        def valid_order_amounts(self) -> bool:
            """Returns True if both subtotal and tax amount are non-negative."""
            return (self.subtotal or 0) >= 0 and (self.tax_amount or 0) >= 0

        @property
        def total_amount(self) -> float:
            """Calculate total order amount including tax by adding subtotal and tax amount."""
            return (self.subtotal or 0.0) + (self.tax_amount or 0.0)

        @property
        def is_rush_hour_order(self) -> bool:
            """Returns True if the order was placed during peak hours (7-9 AM or 12-2 PM)."""
            return self.order_hour in [7, 8, 9, 12, 13, 14]

    @define(concepts.LocationDailyMetrics)
    class LocationDailyMetricsExtension:
        @property
        def gross_profit(self) -> float:
            """Calculate daily gross profit as revenue minus cost of goods sold."""
            return (self.total_revenue or 0) - (self.total_cogs or 0)

        @property
        def labor_cost_percentage(self) -> Optional[float]:
            """Calculate labor cost as percentage of total revenue."""
            if not self.total_revenue or self.total_revenue == 0:
                return None
            return (self.total_labor_cost / self.total_revenue) * 100

        @property
        def avg_wait_time_threshold(self) -> bool:
            """Returns True if average wait time is within acceptable threshold of 5 minutes (300 seconds)."""
            return (self.avg_wait_time_seconds or 0) <= 300

    @define(concepts.Shift)
    class ShiftExtension:
        @property
        def violates_overtime_limit(self) -> bool:
            """Check if the barista linked to this shift has exceeded 8 overtime_hours within any rolling 7-day window that includes this shift's date."""
            if not hasattr(self, "barista") or not hasattr(self, "date"):
                return False

            # Get all shifts for this barista within 7 days before and after this shift's date
            window_shifts = [
                shift
                for shift in self.barista.shifts
                if hasattr(shift, "date")
                and hasattr(shift, "overtime_hours")
                and abs((shift.date - self.date).days) <= 7
            ]

            # Check each possible 7-day window that includes this shift's date
            for i, shift in enumerate(window_shifts):
                window_start = shift.date
                window_end = window_start + timedelta(days=6)

                # Sum overtime hours for shifts within this 7-day window
                total_overtime = sum(
                    s.overtime_hours
                    for s in window_shifts
                    if window_start <= s.date <= window_end
                    and s.overtime_hours is not None
                )

                if total_overtime > 8:
                    return True

            return False

    return concepts, source_tables
