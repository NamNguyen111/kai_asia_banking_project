"""
Segmentation rules:
Bank có 6 segments:
- Iron, Silver, Gold, Platinum, Diamond, Private
Iron: Người dùng bình thường
Silver: 
Thỏa mãn 1 trong 3 điều kiện sau:
- Tổng tài sản bình quân 3 tháng gần nhất từ 100tr đến 200tr, tính bao gồm:
    - Tài khoản thanh toán (Checking account)
    - Tiền gửi (tiền gửi tiết kiệm (Saving account), tiền gửi có kì hạn (Term Deposit), chứng chỉ tiền gửi(Certificate of Deposit))
    - Trái phiếu (Bond) và chứng chỉ quỹ(Fund certificate) tại Techcom Securities
- Tổng tiền gửi mới trong 1 ngày từ 100 triệu, tiền gửi bao gồm:
    - Tiền gửi tiết kiệm (kỳ hạn tối thiểu 3 tháng) --> Là tiền vào tài khoản Savings
    - Tiền gửi có kỳ hạn (kỳ hạn tối thiểu 3 tháng) --> Là tiền vào Term Deposit
    - Chứng chỉ tiền gửi (không bao gồm chứng chỉ tiền gửi trong phần số dư sinh lời tự động) --> Là tiền vào Certificate of Deposit
- Số lượng giao dịch chủ động trong 30 ngày gần nhất (trên tài khoản thanh toán, thẻ thanh toán và thẻ tín dụng) từ 
40 giao dịch và SỐ DƯ BÌNH QUÂN CỦA TÀI KHOẢN THANH TOÁN trong 3 tháng gần nhất từ 30 triệu.
Gold:
- Tổng tài sản bình quân 3 tháng gần nhất từ 200tr, tính bao gồm:
    - Tài khoản thanh toán
    - Tiền gửi (tiền gửi tiết kiệm, tiền gửi có kì hạn, chứng chỉ tiền gửi)
    - Trái phiếu và chứng chỉ quỹ tại Techcom Securities
- Tổng giá trị quan hệ tài chính 3 tháng gần nhất từ 250 triệu 
    - Tổng giá trị quan hệ tài chính bao gồm: tổng tài sản bình quân và dư nợ vay có tài sản đảm bảo (không bao gồm
    vay cầm cố giấy tờ có giá, vay cầm cố trái phiếu và thấu chi có tài sản đảm bảo)
- Giá trị giao dịch trái phiếu và chứng chỉ quỹ tại Techcom Securities từ 200 triệu

Platinum:
Thỏa mãn 1 trong 3 điều kiện sau:
- Tổng tài sản bình quân 3 tháng gần nhất từ 1 tỷ đến dưới 18 tỷ VND
    Tài sản được tính bao gồm:
        - Tài khoản thanh toán
        - Tiền gửi (tiền gửi tiết kiệm, tiền gửi có kỳ hạn, chứng chỉ tiền gửi)
        - Trái phiếu và chứng chỉ quỹ tại Techcom Securities
- Tổng giá trị quan hệ tài chính 3 tháng gần nhất:
    - Từ 2 tỷ đến dưới 25 tỷ VNĐ
    Tổng giá trị quan hệ tài chính bao gồm tổng tài sản bình quân và dư nợ vay có tài sản đảm bảo (không bao gồm
    vay cầm cố giấy tờ có giá, vay cầm cố trái phiếu và thấu chi có tài sản đảm bảo)
- Tổng tiền gửi mới trong 1 ngày từ 1 tỷ đến dưới 23 tỷ
    Tiền gửi bao gồm: 
    - Tiền gửi tiết kiệm (kỳ hạn tối thiểu 3 tháng)
    - Tiền gửi có kỳ hạn (kỳ hạn tối thiểu 3 tháng)
    - Chứng chỉ tiền gửi (không bao gồm chứng chỉ tiền gửi trong phần số dư sinh lời tự động)
- Giá trị giao dịch trái phiếu và chứng chỉ quỹ tại Techcom Securities từ 1 tỷ - 23 tỷ

Diamond:
- Tổng tài sản bình quân 3 tháng gần nhất từ 18 tỷ đến dưới 23 tỷ
    Tài sản được tính bao gồm:
        - Tài khoản thanh toán
        - Tiền gửi (tiền gửi tiết kiệm, tiền gửi có kỳ hạn, chứng chỉ tiền gửi)
        - Trái phiếu và chứng chỉ quỹ tại Techcom Securities
        
Private:
Thỏa mãn 1 trong 3 điều kiện sau:
- Tổng tài sản bình quân 3 tháng gần nhất từ 23 tỷ 
    Tài sản được tính bao gồm:
    - Tài khoản thanh toán
    - Tiền gửi (tiền gửi tiết kiệm, tiền gửi có kỳ hạn, chứng chỉ tiền gửi)
    - Trái phiếu và chứng chỉ quỹ tại Techcom Securities
- Tổng giá trị quan hệ tài chính 3 tháng gần nhất từ 25 tỷ
    - Tổng giá trị quan hệ tài chính bao gồm: tổng tài sản bình quân và dư nợ vay có tài sản đảm bảo (không bao gồm
    vay cầm cố giấy tờ có giá, vay cầm cố trái phiếu và thấu chi có tài sản đảm bảo)
- Tổng tiền gửi mới trong 1 ngày từ 23 tỷ
    Tiền gửi bao gồm: 
    - Tiền gửi tiết kiệm (kỳ hạn tối thiểu 3 tháng)
    - Tiền gửi có kỳ hạn (kỳ hạn tối thiểu 3 tháng)
    - Chứng chỉ tiền gửi (không bao gồm chứng chỉ tiền gửi trong phần số dư sinh lời tự động)
- Giá trị giao dịch trái phiếu và chứng chỉ quỹ tại Techcom Securities từ 23 tỷ

Thông tin về trái phiếu, chứng chỉ quỹ, chứng chỉ tiền gửi, tiền gửi có kỳ hạn
- Kỳ hạn = chiều dài con đường.
- Đáo hạn = ngày về đích.
- Kỳ hạn trả lãi = các trạm dừng nghỉ, nơi bạn nhận “tiền xăng” (lãi) dọc đường.

Trái phiếu - Bonds: 
- Là một loại chứng khoán nợ. Khi bạn mua trái phiếu, tức là bạn cho doanh nghiệp hoặc chính phủ vay tiền.
- Một trái phiếu thực chất là một chuỗi dòng tiền trong tương lai (future cash flows).
- Kỳ hạn của trái phiếu loại ngắn nhất là từ 1-5 năm, tầm trung từ 5-10 năm.
- 2 loại trái phiếu chính:
    - Trái phiếu chính phủ:
        - Được Bộ Tài chính và Kho bạc Nhà nước quản lý
        - Thanh khoản cao (Nhà nước bảo kê)
        - Lãi thấp hơn (Cái gì an toàn lãi thường thấp)
        - Chính quyền của 1 địa phương cũng có thể phát hành trái phiếu
    - Trái phiếu doanh nghiệp:
        - Lãi cao hơn, tính thanh khoản kém hơn trái phiếu chính phủ
        - Thường là trái phiếu có lãi suất cố định, thả nổi, hoặc là trái phiếu chiết khấu
        - Có thể là trái phiếu chuyển đổi - đổi thành cổ phiếu của công ty
- Có thể bán trái phiếu trước khi đáo hạn.

Chứng chỉ quỹ - Mutual fund certificates:
- Là một loại chứng khoán xác nhận quyền sở hữu của nhà đầu tư đối với phần vốn góp vào quỹ đầu tư chứng khoán.
- Nói ngắn gọn: Mình góp tiền vào 1 quỹ đầu tư -> quỹ đó đem tiền đi đầu tư.
- Không có kỳ hạn cố định, tính thanh khoản phụ thuộc vào thị trường.
- Có 3 loại chính ở VN:
    - Quỹ mở (Open-ended fund):
        - Nhà đầu tư có thể mua bán chứng chỉ quỹ trực tiếp với công ty quản lý quỹ vào bất kỳ ngày giao dịch nào.
        - Thanh khoản cao
    - Quỹ đóng (Close-ended fund):
        - Phát hành chứng chỉ quỹ một lần, niêm yết trên sàn chứng khoán.
        - NĐT muốn mua bán thì giao dịch trên sàn như cổ phiếu, tính thanh khoản phụ thuộc cầu của thị trường.
    - Quỹ ETF (Exchange-Traded Fund):
        - Là biến thể của quỹ mở, được niêm yến trên sàn chứng khoán -> NĐT có thể mua bán ETF như cổ phiếu.
    Do NĐT chỉ tham gia góp vốn -> nếu đơn vị đầu tư không khôn ngoan có thể trực tiếp làm giảm giá trị của chứng chỉ quỹ -> lỗ.
    NAV = Net asset value: Giá trị tài sản ròng (ở đây nói về tổng giá trị của cả quỹ)

Chứng chỉ tiền gửi - Certificate of deposit(CD):
- Là giấy tờ xác nhận bạn gửi một khoản tiền tại ngân hàng trong một kỳ hạn cố định.
- Ngân hàng sẽ cam kết trả lãi suất cố định và trả lại gốc khi đáo hạn.
- Có thể chuyển nhượng cho người khác (tùy ngân hàng), nếu rút trước hạn, bị phạt.
- Kỳ hạn 6-60 tháng, hoặc dài hơn.
- Lãi suất fixed, thường cao hơn gửi tiền thông thường.

Tiền gửi có kỳ hạn - Term deposit:
- Là hình thức bạn gửi một khoản tiền tại ngân hàng trong một khoảng thời gian xác định (kỳ hạn), và ngân hàng trả lãi theo thỏa thuận.
- Kỳ hạn phổ biến: 1 tháng, 3 tháng, 6 tháng, 12 tháng, 24 tháng.
- Rút tiền trước hạn = chịu mất một phần hoặc toàn bộ lãi.
- Thanh khoản thấp vì rút = phạt.
"""