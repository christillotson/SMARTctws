def generate_query_and_params(serialIds, species_ids, datemin, datemax):

    sql = """
    SELECT tObservations.*, tAnimal.species_id, tAnimal.first_scraped, tAnimal.last_scraped, tSpecies.species_name
    FROM tObservations
    JOIN tAnimal ON tObservations.serialId = tAnimal.serialId
    JOIN tSpecies ON tAnimal.species_id = tSpecies.species_id
    WHERE 1=1
    """

    params = {}

    # --------------------------
    # serialId handling
    # --------------------------
    if serialIds is not None:
        if isinstance(serialIds, list) and len(serialIds) > 1:
            placeholders = []
            for i, sid in enumerate(serialIds):
                key = f"serial{i}"
                params[key] = sid
                placeholders.append(f":{key}")
            sql += f" AND tObservations.serialId IN ({', '.join(placeholders)})"
        else:
            sql += " AND tObservations.serialId = :serialId"
            params["serialId"] = serialIds[0] if isinstance(serialIds, list) else serialIds

    # --------------------------
    # species_id handling
    # --------------------------
    if species_ids is not None:
        if isinstance(species_ids, list) and len(species_ids) > 1:
            placeholders = []
            for i, sid in enumerate(species_ids):
                key = f"sp{i}"
                params[key] = sid
                placeholders.append(f":{key}")
            sql += f" AND tAnimal.species_id IN ({', '.join(placeholders)})"
        else:
            sql += " AND tAnimal.species_id = :species_id"
            params["species_id"] = species_ids[0] if isinstance(species_ids, list) else species_ids

    # --------------------------
    # date range
    # --------------------------
    sql += " AND date >= :datemin AND date <= :datemax"
    params["datemin"] = datemin
    params["datemax"] = datemax

    return sql, params

