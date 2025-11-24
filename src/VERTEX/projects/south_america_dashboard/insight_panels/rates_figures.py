import pandas as pd

import vertex.IsaricDraw as idw


def define_button():
    """Defines the button in the main dashboard menu"""
    # Insight panels are grouped together by the button_item. Multiple insight
    # panels can share the same button_item are grouped in the dashboard menu
    # according to this
    # However, the combination of button_item and button_label must be unique
    button_item = "Figures"
    button_label = "Database Rates"
    output = {"item": button_item, "label": button_label}
    return output


def create_visuals(df_map, df_forms_dict, dictionary, quality_report, filepath, suffix, save_inputs):
    """
    Create all visuals in the insight panel from the RAP dataframe
    """
    
    return ()