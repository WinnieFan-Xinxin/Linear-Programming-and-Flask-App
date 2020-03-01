
import pandas as pd
from pulp import *
import numpy as np
import datetime
from datetime import datetime as dtm
import time
from pprint import pprint as pp
import warnings
import argparse
import os
from collections import namedtuple
#from pythoncom import com_error
import xml.etree.ElementTree as ET
import xml


directory = os.getcwd()
os.chdir(directory)
def run_model(supplycap,supplycost,arccap,arccost,arcmin,dmd,tariff_surc):

    # Add one more column for every dataframe

    dataframelist=[supplycap,supplycost,arccap,arccost,arcmin,dmd]
    #dataframelist=[supplycap,supplycost,arccap,arccost]
    for dataframe in dataframelist:
        dataframe['date']=pd.to_datetime(dataframe['date'])
        dataframe['date']=pd.to_datetime(dataframe['date'],errors='coerce',format = '%Y-%m-%d').dt.date
        dataframe['str_date']=dataframe['date'].apply(lambda x: x.strftime('%m-%Y'))

    # Input Supply - supply capacity data
    ## pickup data
    supply_cap,stringy_dates, actual_dates = _get_restricted_data(supplycap)
    # get ref cols
    supply_cap_cols = supply_cap.columns.values.tolist()
    ref_cols = get_ref_cols(supply_cap_cols,['str_date','date','capacity'])

    # remember the unique values for date & ref cols in the supply capacity
    # so that we can validate the cost dataset is aligned
    sup_valid_rules = {'date': actual_dates}
    for sup_col in ref_cols:
        sup_valid_rules[sup_col] = supply_cap[sup_col].unique()

    # now supply pricing data
    supply_cost = _get_restricted_data(supplycost,valid_rules=sup_valid_rules)[0]

    # validate data shape (i.e. number of rows/ cols)
    if supply_cost.shape != supply_cap.shape:
        raise ValueError('misaligned data structure detected in '
                         'tbl_supply_capacity vs tbl_supply_cost'
                         ' (they must be indentical)')

    # merge supply capacity and cost
    supply = pd.merge(supply_cap, supply_cost, 'inner',
                      on=ref_cols.append('str_date'))

    ## arc capacity
    arc_valid_rules = {'date': actual_dates}
    arc_cap = _get_restricted_data(arccap,valid_rules=arc_valid_rules)[0]
    arc_cap_cols = arc_cap.columns.values.tolist()
    arc_ref_cols = get_ref_cols(arc_cap_cols,['str_date','date','capacity'])

    # arc costs
    for col in ['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub', 'to_hub','arc_name']:
        arc_valid_rules[col] = arc_cap[col].unique()
    arc_cost = _get_restricted_data(arccost,valid_rules=arc_valid_rules)[0]
    arc_cost_cols = arc_cost.columns.values.tolist()
    arc_cost_ref_cols = get_ref_cols(arc_cost_cols,['str_date','date','cost_pesoGJ'])
    
    arc_valid_rules1 = {'date': actual_dates}
    arc_min_flow = _get_restricted_data(arcmin,valid_rules=arc_valid_rules1)[0]
    arc_min_flow.min_flow = arc_min_flow.min_flow.astype(float)

    # only validate the number of rows, because arc cap can have more columns
    if arc_cost.shape[0] != arc_cap.shape[0]:
        raise ValueError('misaligned data structure detected in '
                         'tbl_arc_cost vs tbl_arc_capacity '
                         '(they must be aligned)')

    # merge arc capacity & cost
    arcs = pd.merge(arc_cap, arc_cost, 'inner',
                    on=arc_cost_ref_cols.append('str_date'))

    # calculate arc tariffs
    tariff_surcharges = tariff_surc
    arcs['join_key'] = 1
    # need a temporary dummy column called join_key to link the tables together
    tariff_surcharges['join_key'] = 1
    # this next merge will multiply the number of records in arcs by
    # however many records there are in tariff_surcharges
    # (it's essentially an outer join)
    arcs = pd.merge(arcs, tariff_surcharges, on='join_key', how='inner')
    arcs.drop('join_key', axis=1, inplace=True)
    arcs['multiplier']=pd.to_numeric(arcs['multiplier'])
    arcs['capacity_portion']=pd.to_numeric(arcs['capacity_portion'])
    
    arcs['capacity'] = arcs['capacity'] * arcs['capacity_portion']

    # bring in peso exchange rate
    # TODO: remove the date_map from here
    # (it's already in _get_unpivoted_data')
    date_map = dict(zip(stringy_dates, actual_dates))

    # fx_rate = get_df_from_table('Pipeline Tariff', 'tbl_FXrate')
    # fx_rate = fx_rate.transpose().reset_index().iloc[1:]
    # fx_rate.rename(columns={'index': 'str_date', 0: 'pesoUSD'},inplace=True)
    # fx_rate['date'] = fx_rate['str_date'].apply(lambda x: date_map[x])
    # arcs = pd.merge(arcs, fx_rate, how='inner', on=['str_date', 'date'])
    arcs['cost_pesoGJ'] = arcs['cost_pesoGJ'] * arcs['multiplier']
    
    if (arcs['topology'] =='Southern Cone').all() == True: 
        arcs['cost_USDmmBtu'] = arcs['cost_pesoGJ'].astype(np.float64).round(4)
    else:
        # convert to USD/mmBtu
        # GJ per mmBtu constant, source: ISO 80000-5
        gj_mmBtu=1.055056 
        pesoUSD = 18.5
        arcs['cost_USDmmBtu'] = arcs['cost_pesoGJ'] / pesoUSD * gj_mmBtu
        arcs['cost_USDmmBtu'] = arcs['cost_USDmmBtu'].astype(np.float64).round(4)

    # demand data
    dmd_valid_rules = {'date': actual_dates}
    demand = _get_restricted_data(dmd,valid_rules=dmd_valid_rules)[0]
    # by this point we have all our input data (supply, arcs, minflows, demand)

    # PROCESS
    tStart = dtm.now()
    results = solve_network(supply, arcs, arc_min_flow, demand, actual_dates)
    tEnd = dtm.now()
    print ("Run nemo model : " + str(tEnd-tStart))    

    # PREPARE OUTPUT DATA (DATA MANIPULATION)
    supply.set_index(['node', 'hub', 'date'], inplace=True)
    # combine source data & solved data to ensure we have all the data
    full_solved_supply = pd.merge(supply, results.production, left_index=True,
                                  right_index=True)

    arcs.set_index(['from_hub', 'to_hub', 'tranche', 'date'], inplace=True)
    full_solved_arcs = pd.merge(arcs, results.flows, left_index=True,
                                right_index=True)
    full_solved_arcs.set_index(['Unique_From_Hub_ID','Unique_To_Hub_ID','arc_name','case_id','topology','str_date'], append=True, inplace=True)
    full_solved_arcs = full_solved_arcs[['capacity', 'flow']]

    # aggregate all the tranches together
    full_solved_arcs = full_solved_arcs.groupby(level=['Unique_From_Hub_ID','Unique_To_Hub_ID','from_hub', 'to_hub','arc_name', 'date','str_date','case_id','topology']).sum()

    full_solved_arcs['utilisation'] = full_solved_arcs['flow']/full_solved_arcs['capacity']
    #full_solved_arcs['utilisation'] = [full_solved_arcs['flow'][x]/full_solved_arcs['capacity'][x] if
    #                                  full_solved_arcs['capacity'][x]!=0 else 0 for x in 
    #                                  range(0,len(full_solved_arcs))]

    # demand
    demand.set_index(['node', 'date'], inplace=True)
    full_solved_demand = pd.merge(demand, results.prices, left_index=True,
                                  right_index=True)
    
    # optimal or not
    solver_status = results.solver_info

    # pivot production
    # pvt_prod = results.production.reset_index().pivot(index='node',columns='date',values='production')

    # pvt_flows
    # pvt_flows = pd.pivot_table(full_solved_arcs.reset_index(),values='flow', index=['from_node','to_node', 'name'],columns='date')

    # utilisation
    # pvt_utilisation_hh = pd.pivot_table(full_solved_arcs.reset_index(),values='utilisation',index=['from_node', 'to_node', 'name'],columns='date')

    # pvt_prices
    # pvt_prices = pd.pivot_table(full_solved_demand.reset_index(),values='price', index=['hub'], columns='date',aggfunc=np.amax)

    # dict of dfs for our send_csvs function
    """
    more_outputs = {
        'full_solved_arcs': full_solved_arcs,
        'full_solved_supply': full_solved_supply,
        #'pivoted_production': pvt_prod,
        #'pivoted_flows': pvt_flows,
        #'pivoted_utilisation_hh': pvt_utilisation_hh,
        'full_solved_demand': full_solved_demand
    }
    """


    return full_solved_supply,full_solved_demand,full_solved_arcs,solver_status


def solve_network(supply, arcs, arc_min_flow,demand, dates):
    """
        runs the linear optimisation model for each year

        parameters:
            :supply:pandas df (TODO: more info about requirements)
            :arcs:  pandas df (TODO: more info about requirements)
            :arc_min_flow: pandas df
            :demand:pandas df (TODO: more info about requirements)
            :dates: list of unique dates

        returns:
            results:    namedtuple of dataframes
                        (production, flows, solver_info, prices)
    """

    # since we package up run_opm we need to tell it where the cbc.exe lives
    
    # it'll be in the cwd since all exes are on same level
    solverdir = os.path.join(os.getcwd(), 'nemo_env\\Lib\\site-packages\\pulp\\solverdir\\cbc\\win\\64\\cbc.exe')

    solver = COIN_CMD(path=solverdir)

    for date in dates:  # for each month
        # filter each df to the the month in question
        supply_m = supply[supply['date'] == date].copy()
        arcs_m = arcs[arcs['date'] == date].copy()
        arc_min_flow_m = arc_min_flow[arc_min_flow['date'] == date].copy()

        demand_m = demand[demand['date'] == date].copy()
        # demand

        # get all potential suppliers for this year

        # --------------MODEL CREATION/CONFIG-----------------
        # Variables
        suppliers = supply_m['node'].unique().tolist()
        hubs = sorted(set(list(arcs_m['from_hub']) + list(arcs_m['to_hub'])))
        arcs_sh = [tuple(x) for x in supply_m[['node', 'hub']].values]
        lpvar_sales = dict()
        for arc_sh in arcs_sh:
            sup, hub = arc_sh
            lpvar_sales_name = 'Flow_sh_{}_{}'.format(sup, hub)
            # lpvar_sales needs to be a nested dictionary
            # of {supplier:{hub:lpvar}}
            lpvar_sales[sup] = {hub: LpVariable(lpvar_sales_name, 0)}

        demanders = sorted(set(list(demand_m["node"])))
        lpvar_flow_hd = LpVariable.dicts('Flow_hd', (hubs, demanders), 0)

        from_h = arcs_m['from_hub'].unique().tolist()
        to_h = arcs_m['to_hub'].unique().tolist()
        tranches = arcs_m['tranche'].unique().tolist()
        lpvar_flow_hh = LpVariable.dicts('Flow_hh', (from_h, to_h, tranches), 0)

        # Declare model
        prob = LpProblem('MiniLP', LpMinimize)

        # Equations
        # cost of supply
        costs_s = supply_m.set_index('node')['cost'].to_dict()
        eqn_cost_s = [lpvar_sales[s][h] * costs_s[s] for (s, h) in arcs_sh]

        # cost of transit
        costs_hh = arcs_m.set_index(['from_hub', 'to_hub', 'tranche'])[
            'cost_USDmmBtu'].to_dict()
        eqn_cost_hh = [lpvar_flow_hh[hin][hout][tranche] * cost
                       for (hin, hout, tranche), cost in costs_hh.items()]

        # add objective function to problem
        prob += lpSum(eqn_cost_s) + lpSum(eqn_cost_hh), 'Sum_Costs'

        # supply maximum constraints for each supply node
        cap_s = supply_m.set_index('node')['capacity'].to_dict()
        for (s, h) in arcs_sh:
            prob += lpvar_sales[s][h] <= cap_s[s], 'CapC_%s' % s

        # demand minimum constraints
        demand_hd = demand_m.set_index(['hub', 'node'])[
                'demand'].to_dict()
        for (h, d) in demand_hd:
            prob += lpvar_flow_hd[h][d] >= demand_hd[(h, d)], "DemC_%s" % d

        # arc capacity constraints
        cap_hh = arcs_m.set_index(['from_hub', 'to_hub', 'tranche'])[
                'capacity'].to_dict()

        for (hin, hout, tranche) in cap_hh:
            id_hh = (hin, hout, tranche)  # unique id (tuple)
            cap_constraint = lpvar_flow_hh[hin][hout][tranche] <= cap_hh[id_hh]
            # capacity
            prob += cap_constraint, 'ArcC_%s' % '_'.join(id_hh)

        # min flow (which isn't specific to tranche)
        min_hh = arc_min_flow_m.set_index(['from_hub', 'to_hub'])[
                'min_flow'].to_dict()  # minflow doesn't have tranche
        tranches = arcs_m['tranche'].unique()
        for (hin, hout) in min_hh:
            id_hh = (hin, hout)
            if min_hh[id_hh] > 0:
                #print(min_hh)
                min_constraint = lpSum(lpvar_flow_hh[hin][hout][t]
                                       for t in tranches) >= min_hh[id_hh]
                prob += min_constraint, 'ArcMin_%s' % '_'.join(id_hh)
            # else:
                # nothing - ignore zero min flows for efficiency

                
        # add the hub mass balance constraint
        def uniquify(series):
            """shortcut to assign unique series values to a list"""
            return series.unique().tolist()

        for h in hubs:
            in_hubs = uniquify(arcs_m[arcs_m['to_hub'] == h]['from_hub'])
            out_hubs = uniquify(arcs_m[arcs_m['from_hub'] == h]['to_hub'])
            hub_suppliers = uniquify(supply_m[supply_m['hub'] == h]['node'])
            hub_demanders = uniquify(demand_m[demand_m['hub'] == h]['node'])

            hflows_sh = [lpvar_sales[s] for s in hub_suppliers]
            hflows_in_hh = [lpvar_flow_hh[in_h][h] for in_h in in_hubs]
            # ohh the hokey cokey
            hflows_out_hh = [lpvar_flow_hh[h][out_h] for out_h in out_hubs]
            # ohhhhhhhhh the hokey cokey
            # lpvar_flow_hh is the model variable for hub to hub flows
            # it's referenced in the format [from_node][to_node]
            # the previous 2 lines ; 
            hflows_hd = [lpvar_flow_hd[h][d] for d in hub_demanders]

            prob += lpSum(hflows_sh) + lpSum(hflows_in_hh) \
                == lpSum(hflows_hd) + lpSum(hflows_out_hh), 'HMBC_%s' % h

        # solve the model
        prob.writeLP('{}\\MiniLP_nemo.lp'.format(directory))
        prob.solve(solver)  # https://xkcd.com/287/
        # print('solved', date) # debug
        # -------------- OUTPUT DATA -------------------
        # SUPPLY
        solved_suppliers = []
        solved_supply_dfs = []
        for sup, hub_dict in lpvar_sales.items():
            # important to recreate supplier list in case sort order changes
            solved_suppliers.append(sup)
            solved_supply_dfs.append(pd.DataFrame.from_dict(hub_dict,
                                                            orient='index'))
        solved_supply = pd.concat(solved_supply_dfs, keys=solved_suppliers)
        solved_supply.rename(columns={0: 'production'}, inplace=True)
        solved_supply['production'] = solved_supply['production'].apply(
            lambda x: x.value())

        # DEMAND PRICES
        constraints = prob.constraints.items()
        dmd_prices = {k[5:]: v.pi for k, v in constraints
                      if k[:4] == 'DemC'}
        solved_prices = pd.DataFrame(dmd_prices, index=['price']
                                     ).transpose()
        # TODO: switch out the dmd nodes to be replaced with the hubs

        # HH_FLOWS
        flow_values = []
        for hleft, hright_dict in lpvar_flow_hh.items():
            for hright, tranche_dict in hright_dict.items():
                for tranche, lpvar_flow in tranche_dict.items():
                    flow_values.append([hleft, hright, tranche,
                                       lpvar_flow.value()])
        solved_flows = pd.DataFrame(data=flow_values,
                                    columns=['from_hub', 'to_hub',
                                             'tranche', 'flow'])
        solved_flows = solved_flows.dropna()

        status = pulp.LpStatus[prob.status]
        obj_value = prob.objective.value()
        model_info = {'status': status, 'total_cost': obj_value, 'date': date}
        solver_info = pd.DataFrame(data=model_info,
                                   columns=model_info.keys(), index=[0])

        # add in the date
        for df in [solved_supply, solved_prices, solved_flows]:
            df['date'] = date

        # sort out our indices
        solved_supply.index.rename(['node', 'hub'], inplace=True)
        solved_supply.set_index(keys=['date'], append=True, inplace=True)
        solved_flows.set_index(keys=['from_hub', 'to_hub', 'tranche',
                                     'date'], inplace=True)
        solved_prices.index.rename('node', inplace=True)
        solved_prices.set_index(keys=['date'], append=True, inplace=True)
        solver_info.set_index(keys=['date'], inplace=True)

        # now to return the dfs
        if date == dates[0]:  # if we're at the first iteration in the loop
            results = namedtuple('NeMo_results', ['production', 'prices',
                                 'flows', 'solver_info'])
            results.production = solved_supply
            results.prices = solved_prices
            results.flows = solved_flows
            results.solver_info = solver_info
        else:  # we just need to append
            results.production = pd.concat([results.production, solved_supply])
            results.prices = pd.concat([results.prices, solved_prices])
            results.flows = pd.concat([results.flows, solved_flows])
            results.solver_info = pd.concat([results.solver_info,
                                             solver_info])
    # loop to next year
    return results

def _get_restricted_data(df, valid_rules=None):   

    stringy_dates = df['str_date'].unique().tolist()
    actual_dates = pd.to_datetime(df['date']).dt.date.unique().tolist()
    

    # validate - our valid_rules dict has a list of cols
    # & permitted unique values
    if valid_rules is not None:
        for col, valid_values in valid_rules.items():
            unique_values = df[col].unique()
            # ^ is a binary xor operator
            wonky_values = [str(x) for x in set(unique_values) ^
                            set(valid_values)]
            if len(wonky_values) > 0:
                raise ValueError('probably values appear in this table but not another (or vice-versa)')
    return df, stringy_dates, actual_dates  

# list a set of values for 
def get_ref_cols(col_name_list, sublist):
    sublist_as_set = list(sublist)
    return [ x for x in col_name_list if x not in sublist_as_set ]

#---------------------------------------------------------------------------

def _get_restricted_df(df, valid_rules=None):   

    stringy_dates = df['str_date'].unique().tolist()
    actual_dates = pd.to_datetime(df['date']).dt.date.unique().tolist()
    
    # validate - our valid_rules dict has a list of cols
    # & permitted unique values
    valid_rules_sub = {k:v for k, v in valid_rules.items() if k != 'date'}
    if valid_rules is not None:
        for value in valid_rules.get('date'):
            print(value)
            df_sub = df[df['date']==value]
            for col, valid_values in valid_rules_sub.items():
                unique_values = df_sub[col].unique()
                # ^ is a binary xor operator
                wonky_values = [str(x) for x in set(unique_values) ^
                            set(valid_values)]
                if len(wonky_values) > 0:
                    raise ValueError('probably values appear in this table but not another (or vice-versa)')
    return df, stringy_dates, actual_dates  
