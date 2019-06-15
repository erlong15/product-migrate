from mysql import connector
import argparse
import operator as op
from functools import reduce as rd
import re
from transliterate import slugify
import logging
from copy import deepcopy

logger = logging.getLogger('loader')
logger.setLevel(logging.DEBUG)

config = {
    "input": {
        "host": "localhost",
        "user": "root",
        "db_name": "traktors",
    },
    "output": {
        "host": "localhost",
        "user": "root",
        "db_name": "agro",
    },
    "password": "",
    "currency_id": 26,
}


def db_connection(config, db):
    src = connector.connect(host=rd(op.getitem, [db, "host"], config),
                            user=rd(op.getitem, [db, "user"], config),
                            passwd=op.getitem(config, "password"),
                            db=rd(op.getitem, [db, "db_name"], config))
    cur = src.cursor(buffered=True)
    return cur, src


def parse_args():
    p = argparse.ArgumentParser(description='Converter db structure')
    p.add_argument('-p', '--password',
                   help='Root password', required=True)
    args = p.parse_args()
    return args


def create_map_table(cursor, table_name):
    sql_create = f"CREATE TABLE IF NOT EXISTS {table_name}\
     (id INT PRIMARY KEY AUTO_INCREMENT, new_map_id INT, old_map_id INT, \
     INDEX (new_map_id), INDEX (old_map_id) )"
    cursor.execute(sql_create)


def create_map3_table(cursor, table_name):
    sql_create = f"CREATE TABLE IF NOT EXISTS {table_name}\
     (id INT PRIMARY KEY AUTO_INCREMENT, new_map_id INT, \
     middle_map_id INT, old_map_id INT, INDEX (new_map_id), \
     INDEX (middle_map_id), INDEX (old_map_id))"
    cursor.execute(sql_create)


def sql_exec(cur, sql, par=None):
    try:
        if op.truth(par):
            cur.execute(sql, par)
        else:
            cur.execute(sql)
    except connector.Error as err:
        print(err)


def create_debug_t(cursor, t_name):
    sql_create = f"CREATE TABLE IF NOT EXISTS {t_name} \
                  (product_id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL,\
                  product VARCHAR(255) NOT NULL,\
                  manufacturer_id INT(11),\
                  category_id INT(11),\
                  created_at datetime,\
                  product_dlink VARCHAR(255) )"
    cursor.execute(sql_create)


def check_make_tmp(cursor_in, cursor_out, t_name, func):
    sql_existance = f"SHOW TABLES LIKE '{t_name}'"

    cursor_out.execute(sql_existance)
    table_exists = op.truth(cursor_out.fetchall())
    print(table_exists)
    if op.not_(table_exists):
        print(table_exists)
        try:
            func(cursor_in, cursor_out)
        except connector.Error as err:
            print(err)


def parse_dealers(cursor_in, cursor_out):
    sql_select = "select * from dealers"

    sql_insert = ("insert into dealers(dealer, region) "
                  "values (%(dealer)s, %(region)s)")

    sql_insert_map = ("insert into iav_dealers_map(new_map_id, old_map_id) "
                      "values (%(new_map_id)s, %(old_map_id)s)")

    regex = re.compile("(?P<location>(?P<region>.*) "
                       "\\((?P<uic>.*)\\))|^(?P<country>.*)")
    insert_pattern = {"dealer": '', "region": ''}

    unique = {}

    try:
        cursor_in.execute(sql_select)
        request = cursor_in.fetchall()

        create_map_table(cursor_out, "iav_dealers_map")

        for row in request:
            _id, _dealer, _region = row
            match = regex.match(_region).groupdict()
            _dealer = _dealer.capitalize()

            if op.contains(unique.keys(), _dealer):
                mf = deepcopy(_dealer)
                _dealer += f"_{unique[_dealer]}"
                unique[mf] += 1
            else:
                unique.setdefault(_dealer, 1)

            if op.truth(match.get("location", False)):
                if op.eq(match.get("uic", None), u"Россия"):
                    location_data = match.get("region", None)
                else:
                    location_data = match.get("location", None)
            else:
                location_data = match.get("country", None)

            insert_pattern.update({"dealer": _dealer,
                                   "region": location_data})
            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.Error as err:
                print(err)

            insert_map = {"new_map_id": cursor_out.lastrowid,
                          "old_map_id": _id}
            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print(err)

    except Exception as e:
        return type(e).__name__, e.args[0]


def parse_manufacturers(cursor_in, cursor_out):
    sql_select = "select * from manufacturers"

    sql_insert = ("insert into manufacturers(manufacturer, address) "
                  "values (%(manufacturer)s, %(address)s)")

    sql_insert_map = ("insert into iav_manufacturers_map(new_map_id, "
                      "old_map_id) values (%(new_map_id)s, %(old_map_id)s)")

    regex = re.compile("(?P<manufacturer>.*) \\((?P<address>.*)\\)")
    insert_pattern = {"manufacturer": '', "address": ''}

    unique = {}

    try:
        cursor_in.execute(sql_select)
        request = cursor_in.fetchall()

        create_map_table(cursor_out, "iav_manufacturers_map")

        for row in request:
            _id, _name = row
            match = regex.match(_name).groupdict()
            if op.contains(unique.keys(), match["manufacturer"]):
                mf = match["manufacturer"]
                match["manufacturer"] += f"_{unique[match['manufacturer']]}"
                unique[mf] += 1
            else:
                unique.setdefault(match["manufacturer"], 1)

            insert_pattern.update(match)
            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.Error as err:
                print(err)

            insert_map = {"new_map_id": cursor_out.lastrowid,
                          "old_map_id": _id}
            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print(err)

            logger.info(f"{sql_insert_map}, {insert_map}")
    except Exception as e:
        return type(e).__name__, e.args[0]


def parse_category(cursor_in, cursor_out):
    sql_select = ("select * from categories order by cat_id asc, "
                  "cat_parent_id asc")

    sql_sel_whr = "select cat_parent_id from categories where cat_id = %s"

    sql_insert = ("insert into categories(category, parent_id,"
                  " category_dlink) values "
                  "(%(category)s, %(parent_id)s, %(category_dlink)s)")

    sql_insert_map = ("insert into iav_categories_map(new_map_id, "
                      "old_map_id) values (%(new_map_id)s, %(old_map_id)s)")

    tmp_map = {}

    try:
        cursor_in.execute(sql_select)
        request = cursor_in.fetchall()

        create_map_table(cursor_out, "iav_categories_map")

        exclusions = {'opryskivateli': 0, 'sejalki': 0}

        for row in request:
            _id, _pid, _level, _name, _full_name = row
            insert_pattern = {"category": _name,
                              "parent_id": _pid,
                              "category_dlink": slugify(_name)}

            excl_check = op.contains(exclusions.keys(), slugify(_name))

            if op.truth(excl_check):
                excl_counter = exclusions[slugify(_name)]
                exclusions[slugify(_name)] += 1
                excl_value = f"{slugify(_name)}_{excl_counter}"
                op.setitem(insert_pattern,
                           "category_dlink",
                           excl_value)

            if op.ne(_level, 0):

                new_pid = None

                if op.contains([1, 2], _level):
                    new_pid = tmp_map.get(_pid, None)

                if op.ge(_level, 3):
                    try:
                        cursor_in.execute(sql_sel_whr, (_pid,))
                    except connector.Error as err:
                        print(err)
                    # new_pid = cursor_in.fetchone()[0]
                    new_pid = tmp_map.get(cursor_in.fetchone()[0], None)

                    raw_title = [_id, _pid]
                    raw_title = get_category_title(cursor_in,
                                                   raw_title, _pid)
                    str_title = ','.join(['%s'] * len(raw_title))

                    sql_get_title = f"""select
                    group_concat(cat_name separator '/')
                    as title from categories
                    where cat_id in ({str_title})"""

                    cursor_in.execute(sql_get_title, tuple(raw_title))
                    new_title = cursor_in.fetchone()[0]
                    insert_pattern.update({"category": new_title})

                insert_pattern.update({"parent_id": new_pid})

            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.Error as err:
                print(err)
            lri = cursor_out.lastrowid

            insert_map = {"new_map_id": lri, "old_map_id": _id}
            tmp_map[_id] = lri
            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print(err)
    except Exception as e:
        return type(e).__name__, e.args[0]


def get_category_title(cursor_in, raw_title, _pid):
    sql = """select cat_id, cat_parent_id, cat_level
             from categories
             where cat_id = %s"""

    cursor_in.execute(sql, (_pid,))
    next_id, next_pid, level = cursor_in.fetchone()

    if level <= 2:
        raw_title.sort()
        return raw_title

    raw_title.append(next_pid)
    return get_category_title(cursor_in, raw_title, next_pid)


def parse_category_params(cursor_in, cursor_out):
    sql_select = "select * from technical_details_fields"

    sql_insert = """insert into category_params (param_name, param_title) 
                    values (%(param_name)s, %(param_title)s)"""

    sql_insert_map = ("insert into iav_category_params_map(new_map_id, "
                      "old_map_id) values (%(new_map_id)s, %(old_map_id)s)")

    try:
        cursor_in.execute(sql_select)
        request = cursor_in.fetchall()

        create_map_table(cursor_out, "iav_category_params_map")

        for row in request:
            id_, title_ = row
            insert_pattern = {"param_name": title_,
                              "param_title": title_}

            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.IntegrityError as inerr:
                if op.eq(inerr.errno, 1062):
                    insert_pattern["param_name"] = f"{title_}_{id_}"
                    cursor_out.execute(sql_insert, insert_pattern)
            except connector.Error as err:
                print(err)

            lri = cursor_out.lastrowid
            insert_map = {"new_map_id": lri, "old_map_id": id_}

            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print(err)
    except Exception as e:
        return type(e).__name__, e.args[0]


def create_tmp_products(cursor_in, cursor_out):
    sql_select = """select
                        ppi.ppi_id,
                        concat(ppi.ppi_name, ', ', pmf.pm_comment, ': ', pmod.mod_name, ' - ', pmod.mod_id) as ppi_name,
                        pmf.pm_mf_id,
                        ppi.ppi_category,
                        if (pmf.pm_comment = pmod.mod_name,
                        true,
                        false) as correct
                    from
                        ppi_position_import as ppi
                    join positions_manufacturers as pmf on
                        pmf.pm_ppi_id = ppi.ppi_id
                    join positions_models as pmod on
                        pmf.pm_ppi_id = pmod.mod_ppi_id"""

    sql_create = ("CREATE TABLE IF NOT EXISTS tmp_products \
              (id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL,\
              ppi_id INT(11) NOT NULL,\
              ppi_name VARCHAR(255) NOT NULL,\
              pm_mf_id INT(11),\
              ppi_category INT(11),\
              correct BOOLEAN,\
              INDEX (ppi_id), INDEX (pm_mf_id), INDEX (ppi_category),\
              INDEX (correct))")

    sql_insert = ("insert into tmp_products(ppi_id, ppi_name, "
                  "pm_mf_id, ppi_category, correct) values "
                  "(%(ppi_id)s, %(ppi_name)s, "
                  "%(pm_mf_id)s, %(ppi_category)s, %(correct)s)")
    print('create_tmp_products')
    try:
        cursor_in.execute(sql_select)
        request = cursor_in.fetchall()
        print(sql_select)
        sql_exec(cursor_out, sql_create)

        for row in request:
            id_, name_, mf_id_, category_, correct_ = row
            insert_pattern = {"ppi_id": id_,
                              "ppi_name": name_,
                              "pm_mf_id": mf_id_,
                              "ppi_category": category_,
                              "correct": correct_}
            # print(row)
            # print(insert_pattern)
            # continue
            sql_exec(cursor_out, sql_insert, insert_pattern)
    except Exception as e:
        print(e)
        return type(e).__name__, e.args[0]


def make_temporary_prod_with_map(cursor_in, cursor_out):
    sql_mapping = ("insert into tmp_products_with_map (product_old_id, "
                   "product, manufacturer_id, category_id, correct) "
                   "select tprod.ppi_id, tprod.ppi_name as product_name, "
                   "imf.new_map_id as manufacturer_id, "
                   "icat.new_map_id as category_id, tprod.correct "
                   "from tmp_products as tprod "
                   "join iav_manufacturers_map as imf "
                   "on imf.old_map_id = tprod.pm_mf_id join "
                   "iav_categories_map as icat "
                   "on icat.old_map_id = tprod.ppi_category")

    sql_create = ("CREATE TABLE IF NOT EXISTS tmp_products_with_map "
                  "(product_id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL, "
                  "product_old_id INT(11) NOT NULL, "
                  "product VARCHAR(255) NOT NULL, "
                  "manufacturer_id INT(11), "
                  "category_id INT(11), "
                  "correct BOOLEAN, "
                  "INDEX (product_old_id), INDEX (manufacturer_id), "
                  "INDEX (category_id), INDEX (correct))")
    try:
        cursor_out.execute(sql_create)

        create_map3_table(cursor_out, "iav_products_map")

        sql_exec(cursor_out, sql_mapping)

    except Exception as e:
        print(e)


def wrap_products(cursor_in, cursor_out):
    print('wrap_products')
    check_make_tmp(cursor_in, cursor_out, 'tmp_products',
                   create_tmp_products)

    check_make_tmp(cursor_in, cursor_out, "tmp_products_with_map",
                   make_temporary_prod_with_map)


def parse_products(cursor_in, cursor_out):

    sql_select = ("select * from tmp_products_with_map")

    sql_insert = ("insert into products (product, manufacturer_id, "
                  "category_id, product_dlink) values "
                  "(%(product)s, %(manufacturer_id)s, %(category_id)s, "
                  "%(product_dlink)s)")

    sql_insert_map = ("insert into iav_products_map(new_map_id, "
                      "middle_map_id, old_map_id) values (%(new_map_id)s, "
                      "%(middle_map_id)s, %(old_map_id)s)")

    try:
        unique = {}

        cursor_out.execute(sql_select)
        request = cursor_out.fetchall()

        for row in request:
            mid_id_, id_, pname_, mf_id_, cat_id_, _ = row
            insert_pattern = {"product": pname_,
                              "manufacturer_id": mf_id_,
                              "category_id": cat_id_,
                              "product_dlink": slugify(pname_)}

            dlink = insert_pattern["product_dlink"]
            if unique.get(dlink, False):
                insert_pattern["product_dlink"] = f"{dlink}_{unique[dlink]}"
                unique[dlink] += 1
            else:
                unique.setdefault(dlink, 1)

            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.IntegrityError as inerr:
                if op.eq(inerr.errno, 1062):
                    insert_pattern["product"] = f"{pname_}_{id_}"
                    cursor_out.execute(sql_insert, insert_pattern)
                else:
                    print(inerr)
            except connector.Error as err:
                print(err)

            lri = cursor_out.lastrowid
            insert_map = {"new_map_id": lri,
                          "middle_map_id": mid_id_,
                          "old_map_id": id_}

            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print("map", lri, err)
    except Exception as e:
        raise e


def create_tmp_prices(cursor_in, cursor_out):

    def to_decimal(price):
        if op.eq(price, "-"):
            return 0
        return float(price.replace('\xa0', '').replace('руб.', ''))

    sql_select = """select
                pmf.pm_ppi_id,
                pmf.pm_mf_id,
                pmf.pm_price,
                pmf.pm_comment,
                concat(ppi.ppi_name, ', ', pmf.pm_comment, ': ', pmod.mod_name, ' - ', pmod.mod_id) as ppi_name,
                pd.dl_id,
                ppi.ppi_category
            from
                positions_manufacturers as pmf
            join positions_models as pmod on
                pmod.mod_ppi_id = pmf.pm_ppi_id
                and pmf.pm_comment = pmod.mod_name
            join ppi_position_import as ppi on
                ppi.ppi_id = pmf.pm_ppi_id
            join positions_dealers as pd on
                pd.ppi_id = pmf.pm_ppi_id"""

    sql_create = ("CREATE TABLE IF NOT EXISTS tmp_prices \
              (id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL,\
              ppi_id INT(11) NOT NULL,\
              mf_id INT(11),\
              price DECIMAL(14,2),\
              comment VARCHAR(1000),\
              ppi_name VARCHAR(1000),\
              dl_id INT(11) NOT NULL,\
              category INT(11),\
              INDEX (ppi_id), INDEX (mf_id), INDEX (dl_id),\
              INDEX (category))")

    sql_insert = ("insert into tmp_prices (ppi_id, mf_id, "
                  "price, comment, ppi_name, dl_id, category) values "
                  "(%(ppi_id)s, %(mf_id)s, "
                  "%(price)s, %(comment)s, %(ppi_name)s, "
                  "%(dl_id)s, %(category)s)")

    sql_exec(cursor_out, sql_create)

    cursor_in.execute(sql_select)
    request = cursor_in.fetchall()
    for row in request:
        prod_id_, mf_id_, price_, comment_, ppi_name_, dl_id_, category_ = row
        insert_pattern = {"ppi_id": prod_id_,
                          "mf_id": mf_id_,
                          "price": to_decimal(price_),
                          "comment": comment_,
                          "ppi_name": ppi_name_,
                          "dl_id": dl_id_,
                          "category": category_}
        try:
            sql_exec(cursor_out, sql_insert, insert_pattern)
        except connector.Error as err:
            print(insert_pattern, err)


def make_temporary_price_with_map(cursor_in, cursor_out):
    sql_mapping = """insert
                into
                    tmp_prices_with_map (tmp_id,
                    price,
                    product_id,
                    price_options,
                    dealer_id) select
                        tpr.id,
                        tpr.price,
                        ipm.new_map_id as product_id,
                        tpr.ppi_name as price_options,
                        idm.new_map_id as dealer_id
                    from
                        tmp_prices as tpr
                    join iav_manufacturers_map as mf on
                        mf.old_map_id = tpr.mf_id
                    join iav_dealers_map as idm on
                        idm.old_map_id = tpr.dl_id
                    join tmp_products_with_map as tpmap on
                        tpmap.product_old_id = tpr.ppi_id
                        and tpmap.product = tpr.ppi_name
                        and tpmap.manufacturer_id = mf.new_map_id
                    join iav_products_map as ipm on
                        ipm.middle_map_id = tpmap.product_id"""

    sql_create = ("CREATE TABLE IF NOT EXISTS tmp_prices_with_map "
                  "(id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL, "
                  "tmp_id INT(11) NOT NULL, "
                  "price DECIMAL(14,2), "
                  "product_id INT(11), "
                  "price_options VARCHAR(1000), "
                  "dealer_id INT(11), "
                  "INDEX (tmp_id), INDEX (product_id), INDEX (dealer_id))")
    try:
        cursor_out.execute(sql_create)

        create_map3_table(cursor_out, "iav_price_map")

        sql_exec(cursor_out, sql_mapping)

    except Exception as e:
        print(e)


def wrap_prices(cursor_in, cursor_out):
    check_make_tmp(cursor_in, cursor_out, "tmp_prices",
                   create_tmp_prices)

    check_make_tmp(cursor_in, cursor_out, "tmp_prices_with_map",
                   make_temporary_price_with_map)


def parse_prices(cursor_in, cursor_out):

    sql_select = "select * from tmp_prices_with_map"

    sql_insert = ("insert into prices (price, price_options, product_id, "
                  "currency_id, dealer_id) values "
                  "(%(price)s, %(price_options)s, %(product_id)s, "
                  "%(currency_id)s, %(dealer_id)s)")

    sql_insert_map = ("insert into iav_price_map(new_map_id, middle_map_id, "
                      "old_map_id) values (%(new_map_id)s, "
                      "%(middle_map_id)s, %(old_map_id)s)")

    cursor_out.execute(sql_select)
    request = cursor_out.fetchall()
    try:
        for row in request:
            id_, old_id_, price_, product_id_, opt_, dl_id_ = row
            insert_pattern = {"price": float(price_),
                              "price_options": opt_,
                              "product_id": product_id_,
                              "currency_id": config['currency_id'],
                              "dealer_id": dl_id_}
            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.Error as err:
                print(err)
            lri = cursor_out.lastrowid

            insert_map = {"new_map_id": lri,
                          "middle_map_id": id_,
                          "old_map_id": old_id_}
            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print(err)

    except Exception as e:
        raise e


def create_tmp_product_params(cursor_in, cursor_out):
    sql_select = """select
                td.tch_id,
                td.tch_property_id,
                td.tch_property_value,
                pm.ppi_id as product_old_id,
                pm.ppi_name
            from
                technical_details as td
            join (
                    select pmf.pm_ppi_id as ppi_id,
                    concat(ppi.ppi_name, ', ', pmf.pm_comment, ': ', pmod.mod_name, ' - ', pmod.mod_id) as ppi_name,
                    pmod.mod_id
                from
                    positions_manufacturers as pmf
                join positions_models as pmod on
                    pmod.mod_ppi_id = pmf.pm_ppi_id
                    and pmf.pm_comment = pmod.mod_name
                join ppi_position_import as ppi on
                    ppi.ppi_id = pmf.pm_ppi_id) as pm on
                pm.mod_id = td.tch_mod_id"""

    sql_create = """create table if not exists tmp_product_params
                    (id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL,
                    tch_id INT(11),
                    tch_property_id INT(11),
                    tch_property_value VARCHAR(1024),
                    product_old_id INT(11),
                    ppi_name VARCHAR(1024),
                    INDEX(tch_id), INDEX(tch_property_id),
                    INDEX(product_old_id))"""

    sql_insert = """insert
                into
                    tmp_product_params (tch_id,
                    tch_property_id,
                    tch_property_value,
                    product_old_id,
                    ppi_name)
                values (%(tch_id)s,
                %(tch_property_id)s,
                %(tch_property_value)s,
                %(product_old_id)s,
                %(ppi_name)s)"""

    sql_exec(cursor_out, sql_create)

    cursor_in.execute(sql_select)
    request = cursor_in.fetchall()

    for row in request:
        tch_id_, prop_id, prop_val, old_id_, ppi_name_ = row
        insert_pattern = {
            "tch_id": tch_id_,
            "tch_property_id": prop_id,
            "tch_property_value": prop_val,
            "product_old_id": old_id_,
            "ppi_name": ppi_name_
        }

        sql_exec(cursor_out, sql_insert, insert_pattern)


def make_temporary_product_params_with_map(cursor_in, cursor_out):
    sql_mapping = """insert
                into
                    tmp_product_params_with_map (old_id,
                    product_id,
                    param_id,
                    some_value) select
                        tpp.tch_id as old_id,
                        ipm.new_map_id as product_id,
                        cpmap.new_map_id as param_id,
                        tpp.tch_property_value as some_value
                    from
                        tmp_product_params as tpp
                    join iav_category_params_map as cpmap on
                        cpmap.old_map_id = tpp.tch_property_id
                    join tmp_products_with_map as tpmap on
                        tpmap.product_old_id = tpp.product_old_id
                        and tpmap.product = tpp.ppi_name
                    join iav_products_map as ipm on
                        ipm.middle_map_id = tpmap.product_id"""

    sql_create = ("CREATE TABLE IF NOT EXISTS tmp_product_params_with_map "
                  "(id INT(11) PRIMARY KEY AUTO_INCREMENT NOT NULL, "
                  "old_id INT(11) NOT NULL, "
                  "product_id INT(11), "
                  "param_id INT(11), "
                  "some_value VARCHAR(1000), "
                  "INDEX (old_id), INDEX (product_id), INDEX (param_id))")
    try:
        cursor_out.execute(sql_create)

        create_map3_table(cursor_out, "iav_product_params_map")

        sql_exec(cursor_out, sql_mapping)

    except Exception as e:
        print(e)


def wrap_product_params(cursor_in, cursor_out):
    check_make_tmp(cursor_in, cursor_out, 'tmp_product_params',
                   create_tmp_product_params)

    check_make_tmp(cursor_in, cursor_out, "tmp_product_params_with_map",
                   make_temporary_product_params_with_map)


def parse_product_params(cursor_in, cursor_out):
    sql_select = "select * from tmp_product_params_with_map"

    sql_insert_map = ("insert into iav_product_params_map "
                      "(new_map_id, middle_map_id, "
                      "old_map_id) values (%(new_map_id)s, "
                      "%(middle_map_id)s, %(old_map_id)s)")

    cursor_out.execute(sql_select)
    request = cursor_out.fetchall()
    try:
        for row in request:
            id_, old_id_, product_id_, param_id_, some_value_ = row

            try:
                converted = int(some_value_)
                default = "int"
            except ValueError:
                try:
                    converted = float(some_value_)
                    default = "float"
                except ValueError:
                    default = "varchar"
                    converted = some_value_

            sql_insert = f"""insert into product_params (product_id,
                            param_id, {default}_value)
                        values (%(product_id)s,
                        %(param_id)s,
                        %(some_value)s)"""

            insert_pattern = {"product_id": product_id_,
                              "param_id": param_id_,
                              "some_value": converted}

            try:
                cursor_out.execute(sql_insert, insert_pattern)
            except connector.Error as err:
                print(err)
            lri = cursor_out.lastrowid

            insert_map = {"new_map_id": lri,
                          "middle_map_id": id_,
                          "old_map_id": old_id_}
            try:
                cursor_out.execute(sql_insert_map, insert_map)
            except connector.Error as err:
                print(err)

    except Exception as e:
        raise e


def main(args, config):
    op.setitem(config, "password", args.password)

    in_cur, in_conn = db_connection(config, "input")
    out_cur, out_conn = db_connection(config, "output")

    connections = [in_conn, out_conn]
    cursors = [in_cur, out_cur]

    for conn in connections:
        conn.reconnect(attempts=1, delay=0)

    try:
        parsers = [
            # parse_manufacturers,
            # parse_dealers,
            # parse_category,
            # parse_category_params,
            # wrap_products,
            # parse_products,
            # wrap_prices,
            # parse_prices,
            wrap_product_params,
            parse_product_params,
        ]

        for parse_func in parsers:
            parse_func(in_cur, out_cur)
            out_conn.commit()

    except Exception as e:
        out_conn.rollback()
        print(e)
    finally:
        for wrap_obj in [cursors, connections]:
            for obj in wrap_obj:
                obj.close()


if __name__ == '__main__':
    args = parse_args()
    main(args, config)
