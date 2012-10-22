import unittest

from mock import patch


class ThrowAway(object):
    @classmethod
    def run_me(cls):
        cls.i_was_ran = True


class TestGetFunctionPathAndOptions(unittest.TestCase):
    """Make sure junk paths raise exceptions and function args get remapped
    to a path.

    Ensure any specified defaults for functions passed by reference are
    correctly returned.
    """

    def test_valid_name(self):
        """Ensure check job function accepts good input."""
        from furious.job_utils import get_function_path_and_options

        good_names = ['name', 'good.name', 'gr8.name', 'ok.na_me',
                      'Name', 'Good.Name', 'Gr8.Name', 'ok.Na_me']
        for name in good_names:
            cleansed_path, options = get_function_path_and_options(name)
            self.assertEqual(name, cleansed_path)
            self.assertIsNone(options)

    def test_bad_function_path(self):
        """Ensure get_function_path_and_options function raises
        BadFunctionPathError when given a bad path.
        """
        from furious.job_utils import BadFunctionPathError
        from furious.job_utils import get_function_path_and_options

        bad_names = ['', '0abc', 'test.0abc', 'test.ab-cd',
                     'bad%ness', '.nogood']
        for bad_name in bad_names:
            self.assertRaises(
                BadFunctionPathError, get_function_path_and_options, bad_name)

    def test_none_as_function_path(self):
        """Ensure get_function_path_and_options raises BadFunctionPathError
        on missing path.
        """
        from furious.job_utils import BadFunctionPathError
        from furious.job_utils import get_function_path_and_options

        self.assertRaises(
            BadFunctionPathError, get_function_path_and_options, None)

    def test_gets_callable_path(self):
        """Ensure check job function returns the path of a callable."""
        from furious.job_utils import get_function_path_and_options

        def some_function():
            """This will appear to be a module-level function."""
            pass

        path, options = get_function_path_and_options(some_function)
        self.assertEqual('furious.tests.test_job_utils.some_function', path)
        self.assertIsNone(options)

    def test_gets_class_method_path(self):
        """Ensure get function path returns the path of a classmethod."""
        from furious.job_utils import get_function_path_and_options

        path, options = get_function_path_and_options(
            TestGetFunctionPathAndOptions.test_gets_class_method_path)
        self.assertEqual(
            'furious.tests.test_job_utils.TestGetFunctionPathAndOptions.'
            'test_gets_class_method_path', path)
        self.assertIsNone(options)

    def test_gets_logging_path(self):
        """Ensure check job function returns the path of logging callable."""
        from furious.job_utils import get_function_path_and_options
        import logging

        path, options = get_function_path_and_options(logging.info)
        self.assertEqual('logging.info', path)
        self.assertIsNone(options)

    def test_gets_builtin_path(self):
        """Ensure check job function returns the path of built-in callable."""
        from furious.job_utils import get_function_path_and_options

        path, options = get_function_path_and_options(eval)
        self.assertEqual('eval', path)
        self.assertIsNone(options)

    def test_gets_default_options(self):
        """Ensure check job function returns options off a callable."""
        from furious.async import defaults
        from furious.job_utils import get_function_path_and_options

        default_options = {
            'test': 'options'
        }

        @defaults(**default_options)
        def method_with_options():
            """This will appear to be a module-level function."""
            pass

        path, options = get_function_path_and_options(method_with_options)
        self.assertEqual('furious.tests.test_job_utils.method_with_options',
                         path)
        self.assertEqual(default_options, options)

    @unittest.skip('This is just a concept.')
    def test_gets_default_options_from_path(self):
        """Ensure check job function returns options from a path object.

        NOTE: This is just a concept of how this would work.
        """
        from furious.async import FunctionPath
        from furious.job_utils import get_function_path_and_options

        default_options = {
            'test': 'options'
        }

        function_path = FunctionPath("this.is.a.test.function")
        function_path.update_options(default_options.copy())

        # OR maybe:
        # function_path = FunctionPath(
        #     "this.is.a.test.function", default_options.copy())

        path, options = get_function_path_and_options(function_path)
        self.assertEqual('this.is.a.test.function', path)
        self.assertEqual(default_options, options)

    def test_damaged_method_raises(self):
        """Ensure a broken mehtod raises BadFunctionPathError."""
        from furious.job_utils import BadFunctionPathError
        from furious.job_utils import get_function_path_and_options

        class FakeFunk(object):
            def __call__():
                pass

        some_method = FakeFunk()

        self.assertRaisesRegexp(
            BadFunctionPathError, "Unable to determine path to callable.",
            get_function_path_and_options, some_method)


class TestFunctionPathToReference(unittest.TestCase):
    """Test that function_path_to_reference finds and load functions."""

    @patch('__builtin__.dir')
    def test_runs_builtin(self, dir_mock):
        """Ensure builtins are able to be loaded and correctly run."""
        from furious.job_utils import function_path_to_reference

        function = function_path_to_reference("dir")

        self.assertIs(dir_mock, function)

    def test_runs_classmethod(self):
        """Ensure classmethods are able to be loaded and correctly run."""
        from furious.job_utils import function_path_to_reference

        ThrowAway.i_was_ran = False

        function = function_path_to_reference(
            'furious.tests.test_job_utils.'
            'ThrowAway.run_me')

        function()
        self.assertTrue(ThrowAway.i_was_ran)

    def test_raises_on_bogus_builtin(self):
        """Ensure bad "builins" raise an exception."""
        from furious.job_utils import function_path_to_reference
        from furious.job_utils import BadFunctionPathError

        self.assertRaisesRegexp(
            BadFunctionPathError, "Unable to find function",
            function_path_to_reference, "something_made_up")

    @patch('email.parser.Parser')
    def test_runs_std_imported(self, parser_mock):
        """Ensure run_job is able to correctly run bundled python functions."""
        from furious.job_utils import function_path_to_reference

        function = function_path_to_reference("email.parser.Parser")

        self.assertIs(parser_mock, function)

    def test_raises_on_bogus_std_imported(self):
        """Ensure run_job raises an exception on bogus standard import."""
        from furious.job_utils import function_path_to_reference
        from furious.job_utils import BadFunctionPathError

        self.assertRaisesRegexp(
            BadFunctionPathError, "Unable to find function",
            function_path_to_reference, "email.parser.NonExistentThing")
