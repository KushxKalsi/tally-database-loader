create table _diff
(
 guid varchar not null,
 alterid int
);

create table _delete
(
 guid varchar not null
);

create table _vchnumber
(
 guid varchar not null,
 voucher_number varchar
);

create table config
(
 name varchar not null primary key,
 value varchar
);

create table mst_group
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 primary_group varchar,
 is_revenue smallint,
 is_deemedpositive smallint,
 is_reserved smallint,
 affects_gross_profit smallint,
 sort_position int
);

create table mst_ledger
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 alias varchar,
 description varchar,
 notes varchar,
 is_revenue smallint,
 is_deemedpositive smallint,
 opening_balance decimal,
 closing_balance decimal,
 mailing_name varchar,
 mailing_address varchar,
 mailing_state varchar,
 mailing_country varchar,
 mailing_pincode varchar,
 email varchar,
 mobile varchar,
 it_pan varchar,
 gstn varchar,
 gst_registration_type varchar,
 gst_supply_type varchar,
 gst_duty_head varchar,
 bank_account_holder varchar,
 bank_account_number varchar,
 bank_ifsc varchar,
 bank_swift varchar,
 bank_name varchar,
 bank_branch varchar,
 bill_credit_period int
);

create table mst_vouchertype
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 numbering_method varchar,
 is_deemedpositive smallint,
 affects_stock smallint
);

create table mst_uom
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 formalname varchar,
 is_simple_unit smallint,
 base_units varchar,
 additional_units varchar,
 conversion decimal
);

create table mst_godown
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 address varchar
);

create table mst_stock_category
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar
);

create table mst_stock_group
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar
);

create table mst_stock_item
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 category varchar,
 _category varchar,
 alias varchar,
 description varchar,
 notes varchar,
 part_number varchar,
 uom varchar,
 _uom varchar,
 alternate_uom varchar,
 _alternate_uom varchar,
 conversion decimal,
 opening_balance decimal,
 opening_rate decimal,
 opening_value decimal,
 closing_balance decimal,
 closing_rate decimal,
 closing_value decimal,
 costing_method varchar,
 gst_type_of_supply varchar,
 gst_hsn_code varchar,
 gst_hsn_description varchar,
 gst_rate decimal,
 gst_taxability varchar
);

create table mst_cost_category
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 allocate_revenue smallint,
 allocate_non_revenue smallint
);

create table mst_cost_centre
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 category varchar
);

create table mst_attendance_type
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 uom varchar,
 _uom varchar,
 attendance_type varchar,
 attendance_period varchar
);

create table mst_employee
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 id_number varchar,
 date_of_joining date,
 date_of_release date,
 designation varchar,
 function_role varchar,
 location varchar,
 gender varchar,
 date_of_birth date,
 blood_group varchar,
 father_mother_name varchar,
 spouse_name varchar,
 address varchar,
 mobile varchar,
 email varchar,
 pan varchar,
 aadhar varchar,
 uan varchar,
 pf_number varchar,
 pf_joining_date date,
 pf_relieving_date date,
 pr_account_number varchar
);

create table mst_payhead
(
 guid varchar not null primary key,
 alterid int,
 name varchar,
 parent varchar,
 _parent varchar,
 payslip_name varchar,
 pay_type varchar,
 income_type varchar,
 calculation_type varchar,
 leave_type varchar,
 calculation_period varchar
);

create table mst_gst_effective_rate
(
 item varchar,
 _item varchar,
 applicable_from date,
 hsn_description varchar,
 hsn_code varchar,
 duty_head varchar,
 rate decimal,
 rate_per_unit decimal,
 valuation_type varchar,
 is_rcm_applicable smallint,
 nature_of_transaction varchar,
 nature_of_goods varchar,
 supply_type varchar,
 taxability varchar
);

create table mst_opening_batch_allocation
(
 name varchar,
 item varchar,
 _item varchar,
 opening_balance decimal,
 opening_rate decimal,
 opening_value decimal,
 godown varchar,
 _godown varchar,
 manufactured_on date
);

create table mst_opening_bill_allocation
(
 ledger varchar,
 _ledger varchar,
 opening_balance decimal,
 bill_date date,
 name varchar,
 bill_credit_period int,
 is_advance smallint
);

create table trn_closingstock_ledger
(
 ledger varchar,
 _ledger varchar,
 stock_date date,
 stock_value decimal
);

create table mst_stockitem_standard_cost
(
 item varchar,
 _item varchar,
 date date,
 rate decimal
);

create table mst_stockitem_standard_price
(
 item varchar,
 _item varchar,
 date date,
 rate decimal
);

create table trn_voucher
(
 guid varchar not null primary key,
 alterid int,
 date date,
 voucher_type varchar,
 _voucher_type varchar,
 voucher_number varchar,
 reference_number varchar,
 reference_date date,
 narration varchar,
 party_name varchar,
 _party_name varchar,
 place_of_supply varchar,
 is_invoice smallint,
 is_accounting_voucher smallint,
 is_inventory_voucher smallint,
 is_order_voucher smallint
);

create table trn_accounting
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 amount decimal,
 amount_forex decimal,
 currency varchar
);

create table trn_inventory
(
 guid varchar,
 item varchar,
 _item varchar,
 quantity decimal,
 rate decimal,
 amount decimal,
 additional_amount decimal,
 discount_amount decimal,
 godown varchar,
 _godown varchar,
 tracking_number varchar,
 order_number varchar,
 order_duedate date
);

create table trn_cost_centre
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 costcentre varchar,
 _costcentre varchar,
 amount decimal
);

create table trn_cost_category_centre
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 costcategory varchar,
 _costcategory varchar,
 costcentre varchar,
 _costcentre varchar,
 amount decimal
);

create table trn_cost_inventory_category_centre
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 item varchar,
 _item varchar,
 costcategory varchar,
 _costcategory varchar,
 costcentre varchar,
 _costcentre varchar,
 amount decimal
);

create table trn_bill
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 name varchar,
 amount decimal,
 billtype varchar,
 bill_credit_period int
);

create table trn_bank
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 transaction_type varchar,
 instrument_date date,
 instrument_number varchar,
 bank_name varchar,
 amount decimal,
 bankers_date date
);

create table trn_batch
(
 guid varchar,
 item varchar,
 _item varchar,
 name varchar,
 quantity decimal,
 amount decimal,
 godown varchar,
 _godown varchar,
 destination_godown varchar,
 _destination_godown varchar,
 tracking_number varchar
);

create table trn_inventory_additional_cost
(
 guid varchar,
 ledger varchar,
 _ledger varchar,
 amount decimal,
 additional_allocation_type varchar,
 rate_of_invoice_tax decimal
);

create table trn_employee
(
 guid varchar,
 category varchar,
 _category varchar,
 employee_name varchar,
 _employee_name varchar,
 amount decimal,
 employee_sort_order int
);

create table trn_payhead
(
 guid varchar,
 category varchar,
 _category varchar,
 employee_name varchar,
 _employee_name varchar,
 employee_sort_order int,
 payhead_name varchar,
 _payhead_name varchar,
 payhead_sort_order int,
 amount decimal
);

create table trn_attendance
(
 guid varchar,
 employee_name varchar,
 _employee_name varchar,
 attendancetype_name varchar,
 _attendancetype_name varchar,
 time_value decimal,
 type_value decimal
);
