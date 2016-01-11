import aiohttp


def get_form_data(form):
    form_data = aiohttp.MultiDict()
    for el in form.xpath('.//input[@name]|select[@name]|textarea[@name]|button[@name]'):
        data = {}
        if el.tag == 'input':
            if el.attrib.get('type') == 'radio' or el.attrib.get('type') == 'checkbox':
                if el.attrib.get('checked', None):
                    data[el.attrib['name']] = el.attrib.get('value', '')
            else:
                data[el.attrib['name']] = el.attrib.get('value', '')
        elif el.tag == 'select':
            options = el.xpath('./option[@selected]')
            if options:
                data[el.attrib['name']] = options[0].attrib.get('value', '')
        elif el.tag == 'textarea':
            data[el.sttrib['name']] = el.text or ''
        elif el.tag == 'button':
            if el.attrib.get('type', None) == 'submit':
                data[el.attrib['name']] = el.attrib.get('value', '')
        form_data.extend(data)
    return form_data
