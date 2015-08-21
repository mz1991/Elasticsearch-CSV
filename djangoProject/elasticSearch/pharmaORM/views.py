from django.shortcuts import render
from .models import Pharma
# Create your views here.
def post_list(request):
    return render(request, 'pharma/post_list.html', {"data":Pharma.objects.all()})