def test_patient_signals_unique_keys(sample_patient_signals):
    assert sample_patient_signals.set_index(["patient_id","as_of_date"]).index.is_unique
