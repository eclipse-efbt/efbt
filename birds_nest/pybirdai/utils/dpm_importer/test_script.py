def main():
    import mapping_functions as new_maps
    """
    Core Package
    """
    _, framework_map = new_maps.map_frameworks() # frameworks
    _.to_csv("export_debug/mapped/framework.csv",index=False)
    _, domain_map = new_maps.map_domains() # domains
    _.to_csv("export_debug/mapped/domain.csv",index=False)
    _, member_map = new_maps.map_members(domain_id_map=domain_map) # members
    _.to_csv("export_debug/mapped/member.csv",index=False)
    _, dimension_map = new_maps.map_dimensions(domain_id_map=domain_map) # to enumerated variables
    _.to_csv("export_debug/mapped/variable.csv",index=False)
    _, hierarchy_map = new_maps.map_hierarchy(domain_id_map=domain_map) # member hierarchies
    _.to_csv("export_debug/mapped/member_hierarchy.csv",index=False)
    _, hierarchy_node_map = new_maps.map_hierarchy_node(path="target/HierarchyNode.csv", hierarchy_map=hierarchy_map, member_map=member_map) # member hierarchy node
    _.to_csv("export_debug/mapped/member_hierarchy_node.csv",index=False)
    """
    Rendering Package
    """
    _, table_map = new_maps.map_tables(framework_id_map=framework_map)
    _.to_csv("export_debug/mapped/table.csv",index=False)
    _, axis_map = new_maps.map_axis(table_map=table_map)
    _.to_csv("export_debug/mapped/axis.csv",index=False)
    _, ordinate_map = new_maps.map_axis_ordinate(axis_map=axis_map)
    _.to_csv("export_debug/mapped/axis_ordinate.csv",index=False)
    _, cell_map = new_maps.map_table_cell(table_map=table_map)
    _.to_csv("export_debug/mapped/table_cell.csv",index=False)
    _, cell_position_map = new_maps.map_cell_position(cell_map=cell_map,ordinate_map=ordinate_map)
    _.to_csv("export_debug/mapped/cell_position.csv",index=False)

    """
    Data Definition Package
    """

    context_data, context_map = new_maps.map_context_definition(dimension_map=dimension_map,member_map=member_map) # to combination_items (need to improve EBA_ATY and subdomain generation)
    # _, metric_map = new_maps.map_metrics()
    (combination, combination_item), dpv_map = new_maps.map_datapoint_version(context_map=context_map,context_data=context_data) # to combinations and items
    combination.to_csv("export_debug/mapped/combination.csv",index=False)
    combination_item.to_csv("export_debug/mapped/combination_item.csv",index=False)



if __name__ == "__main__":
    main()
