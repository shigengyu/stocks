import sys
import dis

def override(method):
    for super_class in _get_base_classes(sys._getframe(2), method.__globals__):
        if hasattr(super_class, method.__name__):
            if not method.__doc__:
                method.__doc__ = getattr(super_class, method.__name__).__doc__
            return method
    raise AssertionError('No super class method found for "%s"' % method.__name__)

def _get_base_classes(frame, namespace):
    return [_get_base_class(class_name_components, namespace) for class_name_components in _get_base_class_names(frame)]

def _get_base_class_names(frame):
    co, lasti = frame.f_code, frame.f_lasti
    code = co.co_code
    i = 0
    extended_arg = 0
    extends = []
    while i <= lasti:
        c = code[i]
        op = ord(c)
        i += 1
        if op >= dis.HAVE_ARGUMENT:
            oparg = ord(code[i]) + ord(code[i+1])*256 + extended_arg
            extended_arg = 0
            i += 2
            if op == dis.EXTENDED_ARG:
                extended_arg = oparg*long(65536)
            if op in dis.hasconst:
                if type(co.co_consts[oparg]) == str:
                    extends = []
            elif op in dis.hasname:
                if dis.opname[op] == 'LOAD_NAME':
                    extends.append(('name', co.co_names[oparg]))
                if dis.opname[op] == 'LOAD_ATTR':
                    extends.append(('attr', co.co_names[oparg]))
    items = []
    previous_item = []
    for t, s in extends:
        if t == 'name':
            if previous_item:
                items.append(previous_item)
            previous_item = [s]
        else:
            previous_item += [s]
    if previous_item:
        items.append(previous_item)
    return items

def _get_base_class(components, namespace):
    obj = namespace[components[0]]
    for component in components[1:]:
        obj = getattr(obj, component)
    return obj