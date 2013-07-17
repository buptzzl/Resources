# coding=gbk
''' 将 area_mn.dat 映射为统一的格式 '''
ip_area = {}
ip_sub = {}
f = file(r'./area_mn.dat', 'rb')
for l in f:
    line = l.strip(' \t\n,').split(',')
    if len(line)!=3:
        print '[ERROR] line-size, info: ', l
    if line[2]=='86':
        #ip_area[line[0]] = line[1]
        ip_area[line[0]] = line[0]  # 仅仅保持 provinceID, cityID
    elif line[2] == '0':
        continue
    else:
        #ip_sub[line[0]] = "%s,%s" % (line[1].strip("'"), line[2])
        ip_area[line[0]] = line[2]
f.close()

for k in ip_sub:
    val = ip_sub[k].split(',')
    vpar = ip_area[val[1]].strip("'")
    #ip_sub[k] = "%s,%s,86" % (val[0], vpar)

for k in ip_area:
    if k in ip_sub:
        print'[ERROR] sheng in shi: ', k, ip_area[k], ip_sub[k]
    else:
        #ip_sub[k] = '%s,%s,86' % (ip_area[k].strip("'"), ip_area[k].strip("'"))
        pass

f = open(r'./area_mn_map.dat', 'wb')
for k in ip_area:
    f.write('%s,%s\n' % (k,ip_area[k]) )
f.close()

