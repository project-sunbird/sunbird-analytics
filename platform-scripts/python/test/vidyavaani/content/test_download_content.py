import pytest
import os
from download_content import *


def test_unzip_files():
	dir_path = '/Users/ajitbarik/Ilimi/testing/nose/Data/download_content/case1/1463137344367_domain_48617'
	unzip_files(dir_path)
	e_assets_files = ["PopupTint_1460636175572.png", "btn_ok_highlights_1460705843676.png", "icon_hint_1454918891133.png", 
					"micro_345_1463136986_1463136986762.png", "background_1458729298020.png",	
					"friday_345_1463136986_1463136987033.mp3", "icon_home_1459242981364.png", "retryBg_1460727370746.png", 
					"btn_back_1461401700215.png", "goodJob_1460636677521.mp3", "icon_reload_1459243110661.png",	
					"retry_1460636610607.mp3", "btn_next_1461401649059.png", "goodjobBg_1460727428389.png",	
					"icon_submit_1459243202199.png"]
	e_widgets_files = ["1463134898668CustomKeyboard.js", "1463134898808keyboard.css"]
	e_main_files = ["assets", "index.json", "widgets"]
	

	r_main_files  = os.listdir("/Users/ajitbarik/Ilimi/testing/nose/Data/download_content/case1/1463137344367_domain_48617")
	r_widgets_files = os.listdir("/Users/ajitbarik/Ilimi/testing/nose/Data/download_content/case1/1463137344367_domain_48617/widgets")
	r_assets_files = os.listdir("/Users/ajitbarik/Ilimi/testing/nose/Data/download_content/case1/1463137344367_domain_48617/assets")
	
	check = 0
	if sorted(r_main_files) == sorted(e_main_files):
		if sorted(r_widgets_files) == sorted(e_widgets_files):
			if sorted(r_assets_files) == sorted(e_assets_files):
				check = 1

	assert check == 1

def test_copy_main_folders():
	
	pass

def test_add_manifest():
	pass