from django.shortcuts import redirect

def homepage_redirect(request):
    return redirect('pybirdai/')
