mat=load('wlist1.csv');
format long g

for i=1:size(mat,1)
one=mat(i,:);
width=abs(one(3)-one(1));
height=abs(one(4)-one(2));

rectangle('Position',[one(1) one(2) width height] )
end;

