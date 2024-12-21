"""
This module contains a target for testing Redis modules.
"""

def register_extension_info(**kwargs):
    pass

def redis_module_cc_test(name, additional_env_vars = {}, **kwargs):
    """
    Use redis_module_cc_test to run a test that would otherwise be run with cc_test. This target will ensure that ODR violations are not triggered by Redis module API headers.
    """
    native.cc_test(
        name = name,
        linkopts = ["-z muldefs"],
        env = {
            "ASAN_OPTIONS": "detect_odr_violation=0",
        } | additional_env_vars,
        **kwargs
    )

# For build cleaner.
register_extension_info(
    extension = redis_module_cc_test,
    label_regex_for_dep = "{extension_name}",
)
