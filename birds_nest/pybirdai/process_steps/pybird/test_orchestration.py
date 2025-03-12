# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

from pybirdai.process_steps.pybird.orchestration import Orchestration

class TestTable:
    """A simple test class with an init method."""
    test_table = None
    
    def init(self):
        print("TestTable initialized!")
        return None

def test_initialization_tracking():
    """Test that objects are only initialized once."""
    # Reset initialization tracking to start fresh
    Orchestration.reset_initialization()
    
    # Create a test object
    test_obj = TestTable()
    
    # Check that it's not initialized yet
    assert not Orchestration.is_initialized(test_obj), "Object should not be initialized yet"
    
    # Initialize it
    Orchestration().init(test_obj)
    
    # Check that it's now initialized
    assert Orchestration.is_initialized(test_obj), "Object should be initialized now"
    
    # Try to initialize it again - should be skipped
    Orchestration().init(test_obj)
    
    # Reset initialization tracking
    Orchestration.reset_initialization()
    
    # Check that it's no longer considered initialized
    assert not Orchestration.is_initialized(test_obj), "Object should not be initialized after reset"
    
    print("All tests passed!")

if __name__ == "__main__":
    test_initialization_tracking() 