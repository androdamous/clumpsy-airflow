def get_source_name(**kwargs):
    # Retrieve initial input value; default to a sample value if not provided
    input_value = kwargs.get('input_value', 'default_source')
    result = f"Source set to: {input_value}"
    print(result)
    return result
