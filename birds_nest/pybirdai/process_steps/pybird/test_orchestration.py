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

class ReferenceTable:
    """A simple reference table class."""
    def init(self):
        print("ReferenceTable initialized!")
        return None

class TestTable:
    """A simple test class with an init method and a reference."""
    reference_Table = None
    
    def init(self):
        print("TestTable initialized!")
        return None
    
    def calc_referenced_items(self):
        """Test method that uses the reference."""
        if self.reference_Table is None:
            raise AttributeError("'NoneType' object has no attribute 'items'")
        return "Referenced items accessed successfully"

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
    
    # Check that the reference was set
    assert test_obj.reference_Table is not None, "Reference should be set after initialization"
    
    # Try to initialize it again - should be skipped
    Orchestration().init(test_obj)
    
    # Reset initialization tracking
    Orchestration.reset_initialization()
    
    # Check that it's no longer considered initialized
    assert not Orchestration.is_initialized(test_obj), "Object should not be initialized after reset"
    
    print("All tests passed!")

def test_reference_handling():
    """Test that references are properly set even when initialization is skipped."""
    # Reset initialization tracking to start fresh
    Orchestration.reset_initialization()
    
    # Create a test object
    test_obj = TestTable()
    
    # Initialize it
    Orchestration().init(test_obj)
    
    # Check that the reference was set
    assert test_obj.reference_Table is not None, "Reference should be set after initialization"
    
    # Clear the reference to simulate it being lost
    test_obj.reference_Table = None
    
    # Try to initialize it again - should be skipped but references should be set
    Orchestration().init(test_obj)
    
    # Check that the reference was set again
    assert test_obj.reference_Table is not None, "Reference should be set even when initialization is skipped"
    
    # Try to access referenced items
    try:
        result = test_obj.calc_referenced_items()
        print(f"Reference access result: {result}")
    except AttributeError as e:
        assert False, f"Reference access failed: {e}"
    
    print("Reference handling test passed!")

if __name__ == "__main__":
    test_initialization_tracking()
    test_reference_handling() 