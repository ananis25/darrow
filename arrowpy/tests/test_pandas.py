import pytest
import pandas.tests.extension.base as base


# wow, fulfilling in the complete extension array contract will take some digging. 
# Imma head out and just make sure not to do any indexing/stuff with those columns.

class TestCasting(base.BaseCastingTests):
    pass 

class TestConstructor(base.BaseConstructorsTests):
    @pytest.mark.xfail(reason='upstream, pandas has messy code to construct frames/series with empty data. I dont want.')
    def test_construct_empty_dataframe(self, dtype):
        super().test_construct_empty_dataframe(dtype)

class TestDTypes(base.BaseDtypeTests):
    pass

@pytest.mark.xfail(raises=NotImplementedError)
class TestGetItems(base.BaseGetitemTests):
    pass

@pytest.mark.xfail(raises=NotImplementedError)
class TestGroupby(base.BaseGroupbyTests):
    pass

class TestInterfaces(base.BaseInterfaceTests):
    @pytest.mark.xfail(reason='involves a setitem call, I want to keep this array immutable')
    def test_copy(self, data):
        super().test_copy(data)

    @pytest.mark.xfail(reason='involves a setitem call, I want to keep this array immutable')
    def test_view(self, data):
        super().test_view(data)

    @pytest.mark.xfail(reason="not implemented, NumpyExtArray doesn't have NA values")
    def test_isna_extension_array(self, data_missing):
        super().test_isna_extension_array(data_missing)


@pytest.mark.xfail(raises=NotImplementedError)
class TestMethods(base.BaseMethodsTests):
    @pytest.mark.xfail(reason="not implemented")
    def test_value_counts(self, all_data, dropna):
        super().test_value_counts(all_data, dropna)

    @pytest.mark.xfail(reason="not implemented")
    def test_value_counts_with_normalize(self, data):
        super().test_value_counts(data)

    @pytest.mark.xfail(reason="pandas doesn't regard custom types as scalars")
    def test_where_series(self, data, na_value, as_frame):
        super().test_where_series(self, data, na_value, as_frame)


@pytest.mark.xfail(raises=NotImplementedError)
class TestPrinting(base.BasePrintingTests):
    pass


# Truth value when comparing a NumpyExtArray series with another is dubious since we 
# don't have a way to mark an NA value. Remaining tests are related to it in some way. 
